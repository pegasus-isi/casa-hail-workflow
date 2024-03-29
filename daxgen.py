#!/usr/bin/env python

import sys
import os
import pwd
import time
from Pegasus.DAX3 import *
from datetime import datetime
from argparse import ArgumentParser

def get_radar_config(radname):
    radarconf = {
        "arlington.tx": "HC_XUTA.tx.ini",
        "mesquite.tx": "HC_XUTA.tx.ini",
        "ftworth.tx": "HC_XUTA.tx.ini",
        "midlothian.tx": "HC_XMDL.tx.ini",
        "burleson.tx": "HC_KFWS.tx.ini"
    }
    radarassoc = radarconf.get(radname, "HC_XUTA.tx.ini");
    return radarassoc

class single_hail_workflow(object):
    def __init__(self, outdir, nc_fn):
        self.outdir = outdir
        self.nc_fn = nc_fn

    def generate_dax(self):
        "Generate a workflow"
        ts = datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
        dax = ADAG("casa_hail_wf-%s" % ts)
        dax.metadata("name", "CASA Hail")

        for f in self.nc_fn:
            f = f.split("/")[-1]
            if f.endswith(".gz"):
                radarfilename = f[:-3]
                unzip = Job("gunzip")
                unzip.addArguments(f)
                zipfile = File(f)
                unzip.uses(zipfile, link=Link.INPUT)
                unzip.uses(radarfilename, link=Link.OUTPUT, transfer=False, register=False)
                dax.addJob(unzip)
            else:
                radarfilename = f;

            string_end = self.nc_fn[-1].find("-")
            file_time = self.nc_fn[-1][string_end+1:string_end+16]
            file_ymd = file_time[0:8]
            file_hms = file_time[9:15]
            
            #print file_ymd
            #print file_hms
                
            radarfile = File(radarfilename)
            radarloc = self.nc_fn[-1][0:string_end]

            radarconfigfilename = get_radar_config(radarloc)
            radarconfigfile = File(radarconfigfilename)
     
            soundingfilename = "current_sounding.txt"
            soundingfile = File(soundingfilename)
            
            hydroclass_outputcartfilename = radarloc + "-" + file_ymd + "-" + file_hms + ".hc.cart.netcdf"
            hydroclass_outputcartfile = File(hydroclass_outputcartfilename)
            
            radx_configfilename = radarloc + "_latlon.txt"
            radx_configfile = File(radx_configfilename)
            
            if radarloc == 'burleson.tx':
                nexrad_hydroclass_outputfilename = radarloc + "-" + file_ymd + "-" + file_hms + ".hc.netcdf.cfradial"
                nexrad_hydroclass_outputfile = File(nexrad_hydroclass_outputfilename)
                incfradialfilename = radarloc + "-" + file_ymd + "-" + file_hms + ".hc.netcdf.in.cfradial"
                incfradialfile = File(incfradialfilename)
                cfconv_job = Job("RadxConvert")
                cfconv_job.addArguments("-cfradial", "-f", radarfilename, "-outdir", "./", "-outname", incfradialfilename)
                cfconv_job.uses(radarfile, link=Link.INPUT)
                cfconv_job.uses(incfradialfile, link=Link.OUTPUT, transfer=False, register=False)
                radarfilename = incfradialfilename
                radarfile = File(incfradialfilename)
                dax.addJob(cfconv_job)

                nexrad_hydroclass_job = Job("hydroclass")
                nexrad_hydroclass_job.addArguments(radarfilename)
                nexrad_hydroclass_job.addArguments("-c", radarconfigfilename, "-o", nexrad_hydroclass_outputfilename, "-t", "1", "-m", "VHS", "-d", "/opt/hydroclass/membership_functions/", "-s", soundingfilename)
                nexrad_hydroclass_job.uses(radarfile, link=Link.INPUT)
                nexrad_hydroclass_job.uses(radarconfigfile, link=Link.INPUT)
                nexrad_hydroclass_job.uses(nexrad_hydroclass_outputfile, link=Link.OUTPUT, transfer=False, register=False)
                nexrad_hydroclass_job.uses(soundingfile, link=Link.INPUT)
                dax.addJob(nexrad_hydroclass_job)

                nexrad_hydro_grid_job = Job("Radx2Grid")
                nexrad_hydro_grid_job.addArguments("-v", "-params", radx_configfilename, "-f", nexrad_hydroclass_outputfilename, "-outdir", "./", "-outname", hydroclass_outputcartfilename)
                nexrad_hydro_grid_job.uses(radx_configfile, link=Link.INPUT)
                nexrad_hydro_grid_job.uses(nexrad_hydroclass_outputfile, link=Link.INPUT)
                nexrad_hydro_grid_job.uses(hydroclass_outputcartfile, link=Link.OUTPUT, transfer=True, register=False)
                dax.addJob(nexrad_hydro_grid_job)
            else: 
                hydroclass_cfradialfilename = radarloc + "-" + file_ymd + "-" + file_hms + ".hc.netcdf.cfradial"
                hydroclass_cfradialfile = File(hydroclass_cfradialfilename)
                hydroclass_outputfilename = radarloc + "-" + file_ymd + "-" + file_hms + ".hc.netcdf"
                hydroclass_outputfile = File(hydroclass_outputfilename)
                hydroclass_job = Job("hydroclass")
                hydroclass_job.addArguments(radarfilename)
                hydroclass_job.addArguments("-c", radarconfigfilename, "-o", hydroclass_outputfilename, "-t", "1", "-m", "VHS", "-d", "/opt/hydroclass/membership_functions/", "-s", soundingfilename)
                hydroclass_job.uses(radarfile, link=Link.INPUT)
                hydroclass_job.uses(radarconfigfile, link=Link.INPUT)
                hydroclass_job.uses(hydroclass_outputfile, link=Link.OUTPUT, transfer=False, register=False)
                hydroclass_job.uses(soundingfile, link=Link.INPUT)
                hydroclass_job.uses(hydroclass_cfradialfile, link=Link.OUTPUT, transfer=False, register=False)
                #hydroclass_job.profile("pegasus", "label", "label")
                dax.addJob(hydroclass_job)

                hydro_grid_job = Job("Radx2Grid")
                hydro_grid_job.addArguments("-v", "-params", radx_configfilename, "-f", hydroclass_cfradialfilename, "-outdir", "./", "-outname", hydroclass_outputcartfilename)
                hydro_grid_job.uses(radx_configfile, link=Link.INPUT)
                hydro_grid_job.uses(hydroclass_cfradialfile, link=Link.INPUT)
                hydro_grid_job.uses(hydroclass_outputcartfile, link=Link.OUTPUT, transfer=True, register=False)
                dax.addJob(hydro_grid_job)

                netcdf2png_colorscalefilename = "standard_hmc_single.png"
                netcdf2png_colorscalefile = File(netcdf2png_colorscalefilename)
                netcdf2png_outputfilename = radarloc + "-" + file_ymd + "-" + file_hms + "-hmc.png"
                netcdf2png_outputfile = File(netcdf2png_outputfilename)
                
                netcdf2png_job = Job("netcdf2png")
                netcdf2png_job.addArguments("-p", "-39.7,-39.7,0:-39.7,+39.7,0:+39.7,-39.7,0", "-t", "hmc", "-c", netcdf2png_colorscalefilename, "-q", "245", "-o", netcdf2png_outputfilename)
                netcdf2png_job.addArguments(hydroclass_outputfilename)
                netcdf2png_job.uses(netcdf2png_colorscalefile, link=Link.INPUT)
                netcdf2png_job.uses(hydroclass_outputfile, link=Link.INPUT)
                netcdf2png_job.uses(netcdf2png_outputfile, link=Link.OUTPUT, transfer=True, register=False)
		dax.addJob(netcdf2png_job)
                
        # Write the DAX file
        daxfile = os.path.join(self.outdir, dax.name+".dax")
        dax.writeXMLFile(daxfile)
        print daxfile

    def generate_workflow(self):
        # Generate dax
        self.generate_dax()
        
if __name__ == '__main__':
    parser = ArgumentParser(description="Single Hail Workflow")
    parser.add_argument("-f", "--files", metavar="INPUT_FILE", type=str, nargs="+", help="Filename", required=True)
    parser.add_argument("-o", "--outdir", metavar="OUTPUT_LOCATION", type=str, help="DAX Directory", required=True)

    args = parser.parse_args()
    outdir = os.path.abspath(args.outdir)
    
    if not os.path.isdir(args.outdir):
        os.makedirs(outdir)

    workflow = single_hail_workflow(outdir, args.files)
    workflow.generate_workflow()
