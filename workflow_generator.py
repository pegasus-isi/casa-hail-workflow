#!/usr/bin/env python3
"""CASA Hail workflow generator (Pegasus 5).

Generates workflow.yml with the site, transformation, and replica catalogs
embedded, so it plans with a plain `pegasus-plan -s condorpool -o local
workflow.yml` — no external sites.xml/tc.txt/rc.txt and no --input-dir.

For each radar moment file (<radar>-YYYYMMDD-HHMMSS.netcdf[.gz]):
  1. gunzip decompresses it if needed (host tool, no container)
  2. burleson.tx (NEXRAD KFWS) only: RadxConvert rewrites it to CfRadial
  3. hydroclass classifies hydrometeors (hail/rain/snow) per sweep
  4. Radx2Grid maps the classification onto a cartesian grid
  5. netcdf2png renders it (CASA X-band radars only, per the original DAX)

The radar name prefix selects the hydroclass config and Radx2Grid grid
parameters (see RADAR_CONFIGS / input/*_latlon.txt).
"""

import os
import re
from argparse import ArgumentParser
from datetime import datetime, timezone

from Pegasus.api import (
    OS,
    Arch,
    Container,
    Directory,
    File,
    FileServer,
    Job,
    Operation,
    ReplicaCatalog,
    Site,
    SiteCatalog,
    Transformation,
    TransformationCatalog,
    Workflow,
)

RADAR_CONFIGS = {
    "arlington.tx": "HC_XUTA.tx.ini",
    "mesquite.tx": "HC_XUTA.tx.ini",
    "ftworth.tx": "HC_XUTA.tx.ini",
    "midlothian.tx": "HC_XMDL.tx.ini",
    "burleson.tx": "HC_KFWS.tx.ini",
}
DEFAULT_RADAR_CONFIG = "HC_XUTA.tx.ini"
SOUNDING = "current_sounding.txt"
COLORSCALE = "standard_hmc_single.png"


def radar_config(radar_name):
    return RADAR_CONFIGS.get(radar_name, DEFAULT_RADAR_CONFIG)


class HailWorkflow(object):
    def __init__(self, outdir, radar_files):
        self.outdir = outdir
        self.radar_files = radar_files

    def build_site_catalog(self):
        sc = SiteCatalog()

        base = os.getcwd()
        local = Site("local", arch=Arch.X86_64, os_type=OS.LINUX)
        local.add_directories(
            Directory(
                Directory.SHARED_SCRATCH, os.path.join(base, "scratch")
            ).add_file_servers(
                FileServer("file://" + os.path.join(base, "scratch"), Operation.ALL)
            ),
            Directory(
                Directory.SHARED_STORAGE, os.path.join(base, "output")
            ).add_file_servers(
                FileServer("file://" + os.path.join(base, "output"), Operation.ALL)
            ),
        )

        condorpool = Site("condorpool", arch=Arch.X86_64, os_type=OS.LINUX)
        condorpool.add_condor_profile(universe="vanilla")
        condorpool.add_pegasus_profile(style="condor", data_configuration="condorio")

        sc.add_sites(local, condorpool)
        return sc

    def build_transformation_catalog(self):
        tc = TransformationCatalog()

        # Singularity (not Docker): it pulls docker:// images straight from
        # the registry without a Docker daemon, so the workflow also runs on
        # hosts with only singularity/apptainer installed (e.g. Kubernetes
        # pods, HPC nodes).
        hail_container = Container(
            "single_hail_container",
            Container.SINGULARITY,
            image="docker://casaelyons/single_hail_container",
            image_site="docker_hub",
        )
        tc.add_containers(hail_container)

        # gunzip runs on the execute host directly — no container needed.
        tc.add_transformations(
            Transformation(
                "gunzip",
                site="condorpool",
                pfn="/bin/gunzip",
                is_stageable=False,
                arch=Arch.X86_64,
                os_type=OS.LINUX,
            )
        )
        for name, pfn in (
            ("RadxConvert", "/opt/hydroclass/RadxConvert"),
            ("hydroclass", "/opt/hydroclass/hydroclass"),
            ("Radx2Grid", "/opt/hydroclass/Radx2Grid"),
            ("netcdf2png", "/opt/hydroclass_netcdf2png/netcdf2png"),
        ):
            tc.add_transformations(
                Transformation(
                    name,
                    site="condorpool",
                    pfn=pfn,
                    is_stageable=False,
                    container=hail_container,
                    arch=Arch.X86_64,
                    os_type=OS.LINUX,
                )
            )
        return tc

    def parse_radar_file(self, path):
        """(radar_name, ymd, hms) from <radar>-YYYYMMDD-HHMMSS.<ext>."""
        fn = os.path.basename(path)
        m = re.match(r"(?P<radar>[^-]+)-(?P<ymd>\d{8})-(?P<hms>\d{6})\.", fn)
        if not m:
            raise SystemExit(
                f"error: cannot parse radar/timestamp from '{fn}' "
                "(expected e.g. burleson.tx-20170329-071008.netcdf[.gz])"
            )
        return m.group("radar"), m.group("ymd"), m.group("hms")

    def build_replica_catalog(self):
        rc = ReplicaCatalog()
        input_dir = os.path.abspath("input")

        aux = {SOUNDING, COLORSCALE}
        for path in self.radar_files:
            radar_name, _, _ = self.parse_radar_file(path)
            aux.add(radar_config(radar_name))
            aux.add(radar_name + "_latlon.txt")
        for name in sorted(aux):
            full = os.path.join(input_dir, name)
            if not os.path.isfile(full):
                raise SystemExit(
                    f"error: required config file input/{name} not found"
                )
            rc.add_replica("local", name, "file://" + full)

        for path in self.radar_files:
            rc.add_replica("local", os.path.basename(path), "file://" + path)
        return rc

    def generate_workflow(self):
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        wf = Workflow("casa-hail-wf-%s" % ts)
        wf.add_metadata(name="CASA Hail")

        wf.add_site_catalog(self.build_site_catalog())
        wf.add_transformation_catalog(self.build_transformation_catalog())
        wf.add_replica_catalog(self.build_replica_catalog())

        soundingfile = File(SOUNDING)

        for path in self.radar_files:
            radar_name, ymd, hms = self.parse_radar_file(path)
            fn = os.path.basename(path)
            stem = f"{radar_name}-{ymd}-{hms}"

            if fn.endswith(".gz"):
                radarfile = File(fn[:-3])
                unzip_job = Job("gunzip")
                unzip_job.add_args("--force", fn)
                unzip_job.add_inputs(File(fn))
                unzip_job.add_outputs(
                    radarfile, stage_out=False, register_replica=False
                )
                wf.add_jobs(unzip_job)
            else:
                radarfile = File(fn)

            configfile = File(radar_config(radar_name))
            latlonfile = File(radar_name + "_latlon.txt")

            # burleson.tx is the NEXRAD radar (KFWS): its Level-II data must
            # be rewritten to CfRadial before hydroclass can read it.
            if radar_name == "burleson.tx":
                cfradial = File(f"{stem}.hc.netcdf.in.cfradial")
                conv_job = Job("RadxConvert")
                conv_job.add_args(
                    "-cfradial", "-f", radarfile,
                    "-outdir", "./", "-outname", cfradial.lfn,
                )
                conv_job.add_inputs(radarfile)
                conv_job.add_outputs(
                    cfradial, stage_out=False, register_replica=False
                )
                wf.add_jobs(conv_job)
                radarfile = cfradial

            # hydrometeor classification. Mirrors the original DAX: for the
            # NEXRAD radar hydroclass writes the CfRadial directly (-o), for
            # CASA radars it writes <stem>.hc.netcdf plus a
            # <stem>.hc.netcdf.cfradial side file that feeds Radx2Grid.
            hc_out = None
            if radar_name == "burleson.tx":
                hc_cfradial = File(f"{stem}.hc.netcdf.cfradial")
                hc_job = Job("hydroclass")
                hc_job.add_args(
                    radarfile,
                    "-c", configfile, "-o", hc_cfradial.lfn, "-t", "1",
                    "-m", "VHS",
                    "-d", "/opt/hydroclass/membership_functions/",
                    "-s", soundingfile,
                )
                hc_job.add_inputs(radarfile, configfile, soundingfile)
                hc_job.add_outputs(
                    hc_cfradial, stage_out=False, register_replica=False
                )
                wf.add_jobs(hc_job)
            else:
                hc_out = File(f"{stem}.hc.netcdf")
                hc_cfradial = File(f"{stem}.hc.netcdf.cfradial")
                hc_job = Job("hydroclass")
                hc_job.add_args(
                    radarfile,
                    "-c", configfile, "-o", hc_out.lfn, "-t", "1", "-m", "VHS",
                    "-d", "/opt/hydroclass/membership_functions/",
                    "-s", soundingfile,
                )
                hc_job.add_inputs(radarfile, configfile, soundingfile)
                hc_job.add_outputs(
                    hc_out, stage_out=False, register_replica=False
                )
                hc_job.add_outputs(
                    hc_cfradial, stage_out=False, register_replica=False
                )
                wf.add_jobs(hc_job)

            # map the classification onto a cartesian grid
            cart_out = File(f"{stem}.hc.cart.netcdf")
            grid_job = Job("Radx2Grid")
            grid_job.add_args(
                "-v", "-params", latlonfile, "-f", hc_cfradial.lfn,
                "-outdir", "./", "-outname", cart_out.lfn,
            )
            grid_job.add_inputs(latlonfile, hc_cfradial)
            grid_job.add_outputs(
                cart_out, stage_out=True, register_replica=False
            )
            wf.add_jobs(grid_job)

            # render CASA X-band radars to PNG (the original DAX skips the
            # NEXRAD radar here)
            if radar_name != "burleson.tx":
                colorscale = File(COLORSCALE)
                png_out = File(f"{stem}-hmc.png")
                png_job = Job("netcdf2png")
                png_job.add_args(
                    "-p", "-39.7,-39.7,0:-39.7,+39.7,0:+39.7,-39.7,0",
                    "-t", "hmc", "-c", colorscale, "-q", "245",
                    "-o", png_out.lfn, hc_out,
                )
                png_job.add_inputs(colorscale, hc_out)
                png_job.add_outputs(
                    png_out, stage_out=True, register_replica=False
                )
                wf.add_jobs(png_job)

        wf_path = os.path.join(self.outdir, "workflow.yml")
        wf.write(wf_path)
        print(wf_path)


def resolve_radar_file(name):
    """Absolute path of a radar file given bare name or path."""
    if os.path.isfile(name):
        return os.path.abspath(name)
    candidate = os.path.join("input", os.path.basename(name))
    if os.path.isfile(candidate):
        return os.path.abspath(candidate)
    raise SystemExit(f"error: radar file '{name}' not found (looked in ./input/)")


if __name__ == "__main__":
    parser = ArgumentParser(description="CASA Hail Workflow")
    parser.add_argument(
        "-f",
        "--files",
        metavar="INPUT_FILE",
        type=str,
        nargs="+",
        help="Radar file(s), e.g. burleson.tx-20170329-071008.netcdf "
        "(bare names are resolved in ./input/)",
        required=True,
    )
    parser.add_argument(
        "-o",
        "--outdir",
        metavar="OUTPUT_LOCATION",
        type=str,
        default=".",
        help="Directory to write workflow.yml into (default: project root)",
    )

    args = parser.parse_args()
    outdir = os.path.abspath(args.outdir)
    os.makedirs(outdir, exist_ok=True)

    radar_files = [resolve_radar_file(f) for f in args.files]
    HailWorkflow(outdir, radar_files).generate_workflow()
