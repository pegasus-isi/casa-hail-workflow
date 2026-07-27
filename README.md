# casa-hail-workflow

Pegasus workflow for CASA hail detection. For each radar moment file
(`<radar>-YYYYMMDD-HHMMSS.netcdf[.gz]`), it:

1. decompresses the file if needed (`gunzip`, host tool)
2. `burleson.tx` (NEXRAD KFWS) only: rewrites Level-II data to CfRadial
   (`RadxConvert`)
3. classifies hydrometeors — hail/rain/snow (`hydroclass`, with a
   radar-specific config and the current atmospheric sounding)
4. maps the classification onto a cartesian grid (`Radx2Grid`)
5. renders CASA X-band radars to PNG (`netcdf2png`)

Container jobs run in `casaelyons/single_hail_container` (Singularity,
pulled from Docker Hub) on a site named `condorpool`. The radar name prefix
selects the hydroclass config (`HC_*.ini`) and grid parameters
(`*_latlon.txt`); known radars: `arlington.tx`, `mesquite.tx`, `ftworth.tx`,
`midlothian.tx` (CASA X-band), and `burleson.tx` (NEXRAD KFWS).

A sample NEXRAD scan ships in `input/`
(`burleson.tx-20170329-071410.netcdf`, KFWS 2017-03-29 07:14 UTC, from the
public NOAA archive — the same DFW hail event as the casa-wind test data).

## Usage

Generate `workflow.yml` (catalogs embedded — no external `sites.xml`/`tc.txt`/
`rc.txt` needed):

```sh
./workflow_generator.py -f burleson.tx-20170329-071410.netcdf
```

`-f` accepts one or more radar files (bare names are resolved in
`./input/`). `-o` sets where `workflow.yml` is written (default: project
root).

Plan and submit:

```sh
./plan.sh                 # plans + submits ./workflow.yml
```

or equivalently `pegasus-plan -s condorpool -o local --submit workflow.yml`.

## PegasusAI Studio

This repo is Studio-compatible: clone it into `~/work/workflows/`, and it
appears on the Dashboard. The Generate action introspects
`workflow_generator.py` (declared in `.pegasushub.yml`), and Plan/Submit work
directly on the generated `workflow.yml`.

## Legacy files

`legacy/` holds the pre-5.0 DAX3 generators and external catalogs
(`daxgen.py`, `pp_daxgen.py` for the composite workflow, `run_*.sh`,
`sites.xml`, `tc.txt`, `rc.txt`, `pegasus.properties.nfs`), superseded by
`workflow_generator.py`. They must stay out of the project root:
pegasus-plan picks up `tc.txt`/`rc.txt`/`sites.xml` from the working
directory by default, and those definitions silently shadow the catalogs
embedded in `workflow.yml`.
