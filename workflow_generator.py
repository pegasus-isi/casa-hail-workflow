#!/usr/bin/env python3
"""CASA Hail workflow generator (Pegasus 5).

Generates workflow.yml with the transformation and replica catalogs embedded,
plus pegasus.properties, so it plans with a plain
`pegasus-plan --conf pegasus.properties -s compute -o local workflow.yml` — no
external tc.txt/rc.txt and no --input-dir.

Sites follow pegasus-isi/pegasus-gromacs: jobs run on a site named "compute",
defined by a centrally hosted site catalog (-s access-pegasus.yml, ...;
https://github.com/pegasushub/pegasus-site-catalogs) or by one in
~/.pegasusrc. The generator writes no site catalog; on a plain HTCondor pool
with no site catalog, use -e condorpool (Pegasus builds that site itself).
It does not plan or submit: run the pegasus-plan command it prints, or use
CASA-Hail-Workflow.ipynb.

For each radar moment file (<radar>-YYYYMMDD-HHMMSS.netcdf[.gz]):
  1. gunzip decompresses it if needed
  2. burleson.tx (NEXRAD KFWS) only: RadxConvert rewrites it to CfRadial
  3. hydroclass classifies hydrometeors (hail/rain/snow) per sweep
  4. Radx2Grid maps the classification onto a cartesian grid
  5. netcdf2png renders it (CASA X-band radars only, per the original DAX)

The radar name prefix selects the hydroclass config and Radx2Grid grid
parameters (see RADAR_CONFIGS / input/*_latlon.txt).
"""

import os
import re
import sys
from argparse import ArgumentParser
from datetime import datetime, timezone
from pathlib import Path

from Pegasus.api import (
    OS,
    Arch,
    Container,
    Directory,
    File,
    FileServer,
    Job,
    Operation,
    PegasusClientError,
    Properties,
    ReplicaCatalog,
    Site,
    SiteCatalog,
    Transformation,
    TransformationCatalog,
    Workflow,
)

# Per-tool resource configuration for the transformation catalog.
TOOL_CONFIGS = {
    "gunzip": {"memory": "1 GB", "cores": 1},
    "RadxConvert": {"memory": "2 GB", "cores": 1},
    "hydroclass": {"memory": "4 GB", "cores": 1},
    "Radx2Grid": {"memory": "4 GB", "cores": 1},
    "netcdf2png": {"memory": "2 GB", "cores": 1},
}

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
    wf = None
    sc = None
    tc = None
    rc = None
    props = None

    def __init__(self, outdir, radar_files,
                 container_sif="Apptainer/CASA_Hail_Container.sif"):
        self.outdir = outdir
        self.radar_files = radar_files
        self.wf_dir = str(Path(__file__).parent.resolve())
        self.container_sif = container_sif
        self.shared_scratch_dir = os.path.join(outdir, "scratch")
        self.local_storage_dir = os.path.join(outdir, "output")
        self.dagfile = os.path.join(outdir, "workflow.yml")
        self.propsfile = os.path.join(outdir, "pegasus.properties")

    def write(self):
        """Write workflow.yml (catalogs embedded) and pegasus.properties."""
        if self.sc is not None:
            self.wf.add_site_catalog(self.sc)
        self.wf.add_transformation_catalog(self.tc)
        self.wf.add_replica_catalog(self.rc)
        self.props.write(self.propsfile)
        self.wf.write(self.dagfile)

    # ------------------------------------------------------------------
    # Plan / run / monitor (thin wrappers over the Pegasus API Workflow
    # object, for interactive use e.g. from a Jupyter notebook)
    # ------------------------------------------------------------------
    def plan_submit(self, exec_site_name="compute", raise_errors=False):
        try:
            self.wf.plan(
                conf=self.propsfile,
                dir=os.path.join(self.outdir, "submit"),
                sites=[exec_site_name],
                output_sites=["local"],
                cleanup="leaf",
                verbose=1,
                submit=True,
            )
        except PegasusClientError as e:
            print(e)
            if raise_errors:
                raise

    def status(self):
        try:
            self.wf.status(long=True)
        except PegasusClientError as e:
            print(e)

    def wait(self):
        try:
            self.wf.wait()
        except PegasusClientError as e:
            print(e)

    def statistics(self):
        try:
            self.wf.statistics()
        except PegasusClientError as e:
            print(e)

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------
    def create_pegasus_properties(self, hosted_site_catalog=None):
        self.props = Properties()
        # This is the name of the application for analytics
        self.props["pegasus.metrics.app"] = "CASA-HAIL"
        self.props["pegasus.integrity.checking"] = "none"
        self.props["pegasus.transfer.threads"] = "16"
        if hosted_site_catalog:
            # Use one of Pegasus' centrally hosted site catalogs instead of
            # a locally generated one. pegasus-plan downloads and caches the
            # named file from the catalog repository at plan time.
            # https://pegasus.isi.edu/documentation/reference-guide/catalogs.html#centrally-hosted-site-catalogs
            self.props["pegasus.catalog.site.repo.file"] = hosted_site_catalog

    # ------------------------------------------------------------------
    # Site Catalog
    #
    # Not used by the CLI below — pegasus-plan resolves the site catalog
    # from a centrally hosted one instead (see -s/--hosted-site-catalog and
    # create_pegasus_properties above). Kept for programmatic/notebook use
    # when a self-contained, locally generated HTCondor site catalog is
    # wanted.
    # ------------------------------------------------------------------
    def create_sites_catalog(self, exec_site_name="compute"):
        self.sc = SiteCatalog()

        local = Site("local", arch=Arch.X86_64, os_type=OS.LINUX)
        local.add_directories(
            Directory(
                Directory.SHARED_SCRATCH, self.shared_scratch_dir
            ).add_file_servers(
                FileServer("file://" + self.shared_scratch_dir, Operation.ALL)
            ),
            Directory(
                Directory.LOCAL_STORAGE, self.local_storage_dir
            ).add_file_servers(
                FileServer("file://" + self.local_storage_dir, Operation.ALL)
            ),
        )

        exec_site = Site(exec_site_name, arch=Arch.X86_64, os_type=OS.LINUX)
        exec_site.add_condor_profile(universe="vanilla")
        exec_site.add_pegasus_profile(style="condor", data_configuration="condorio")

        self.sc.add_sites(local, exec_site)

    # ------------------------------------------------------------------
    # Transformation Catalog
    # ------------------------------------------------------------------
    def create_transformation_catalog(self, exec_site_name="compute"):
        self.tc = TransformationCatalog()

        # A local Apptainer .sif built with `apptainer build` from
        # Apptainer/CASA_Hail_Container.def. Pegasus stages the file like any
        # other input, so image_site is the site where the .sif physically
        # lives (the submit host = "local").
        sif_path = (
            self.container_sif
            if os.path.isabs(self.container_sif)
            else os.path.join(self.wf_dir, self.container_sif)
        )
        if not os.path.exists(sif_path):
            print(
                f"warning: Apptainer image not found at {sif_path} — build it "
                f"first with: apptainer build {sif_path} "
                f"Apptainer/CASA_Hail_Container.def",
                file=sys.stderr,
            )
        hail_container = Container(
            "single_hail_container",
            container_type=Container.SINGULARITY,
            image="file://" + sif_path,
            image_site="local",
        )
        self.tc.add_containers(hail_container)

        # gunzip runs in the container too (the image ships /bin/gunzip), so
        # the workflow needs nothing installed on the workers.
        for name, pfn in (
            ("gunzip", "/bin/gunzip"),
            ("RadxConvert", "/opt/hydroclass/RadxConvert"),
            ("hydroclass", "/opt/hydroclass/hydroclass"),
            ("Radx2Grid", "/opt/hydroclass/Radx2Grid"),
            ("netcdf2png", "/opt/hydroclass_netcdf2png/netcdf2png"),
        ):
            self.tc.add_transformations(
                Transformation(
                    name,
                    site=exec_site_name,
                    pfn=pfn,
                    is_stageable=False,
                    container=hail_container,
                    arch=Arch.X86_64,
                    os_type=OS.LINUX,
                ).add_pegasus_profile(
                    memory=TOOL_CONFIGS[name]["memory"],
                    cores=TOOL_CONFIGS[name]["cores"],
                )
            )

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

    # ------------------------------------------------------------------
    # Replica Catalog
    # ------------------------------------------------------------------
    def create_replica_catalog(self):
        self.rc = ReplicaCatalog()
        input_dir = os.path.join(self.wf_dir, "input")

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
            self.rc.add_replica("local", name, "file://" + full)

        for path in self.radar_files:
            self.rc.add_replica("local", os.path.basename(path), "file://" + path)

    # ------------------------------------------------------------------
    # Workflow DAG
    # ------------------------------------------------------------------
    def create_workflow(self):
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        self.wf = wf = Workflow("casa-hail-wf-%s" % ts)
        wf.add_metadata(name="CASA Hail")

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


def resolve_radar_file(name):
    """Absolute path of a radar file given bare name or path."""
    if os.path.isfile(name):
        return os.path.abspath(name)
    candidate = os.path.join("input", os.path.basename(name))
    if os.path.isfile(candidate):
        return os.path.abspath(candidate)
    raise SystemExit(f"error: radar file '{name}' not found (looked in ./input/)")


def main():
    parser = ArgumentParser(
        description="CASA Hail Workflow",
        epilog="Writes workflow.yml and pegasus.properties; it does not plan "
        "or submit. Run the pegasus-plan command it prints, or use "
        "CASA-Hail-Workflow.ipynb.",
    )
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
        help="Directory to write workflow.yml and pegasus.properties into "
        "(default: project root)",
    )
    parser.add_argument(
        "--container-sif",
        metavar="PATH",
        type=str,
        default="Apptainer/CASA_Hail_Container.sif",
        help="Path to the Apptainer .sif image, absolute or relative to the "
        "workflow directory (default: Apptainer/CASA_Hail_Container.sif)",
    )
    parser.add_argument(
        "-s",
        "--hosted-site-catalog",
        metavar="FILE",
        type=str,
        default=None,
        help="Name of a Pegasus centrally hosted site catalog to plan against "
        "(e.g. access-pegasus.yml), instead of a locally generated one. Sets "
        "pegasus.catalog.site.repo.file; see "
        "https://pegasus.isi.edu/documentation/reference-guide/catalogs.html"
        "#centrally-hosted-site-catalogs",
    )
    parser.add_argument(
        "-e",
        "--execution-site-name",
        metavar="STR",
        type=str,
        default="compute",
        help="Execution site name (default: compute; use condorpool on a "
        "plain HTCondor pool with no site catalog)",
    )

    args = parser.parse_args()
    outdir = os.path.abspath(args.outdir)
    os.makedirs(outdir, exist_ok=True)

    radar_files = [resolve_radar_file(f) for f in args.files]
    workflow = HailWorkflow(outdir, radar_files, container_sif=args.container_sif)
    workflow.create_pegasus_properties(hosted_site_catalog=args.hosted_site_catalog)
    workflow.create_transformation_catalog(exec_site_name=args.execution_site_name)
    workflow.create_replica_catalog()
    workflow.create_workflow()
    workflow.write()

    print(f"Workflow written to {workflow.dagfile}")
    # --output-dir: no site catalog defines "local", so Pegasus's built-in
    # local site would otherwise stage outputs to ./wf-output.
    print(
        f"Plan and submit: pegasus-plan --conf {workflow.propsfile} "
        f"--dir {os.path.join(outdir, 'submit')} "
        f"-s {args.execution_site_name} -o local "
        f"--output-dir {workflow.local_storage_dir} --submit {workflow.dagfile}"
    )


if __name__ == "__main__":
    main()
