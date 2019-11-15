#!/usr/bin/env python

import sys
import os
import pwd
import time
import shutil
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

class composite_hail_workflow(object):
    def __init__(self, dax_name, outdir, nc_fn, cart_fn, default_properties, default_replica):
        self.outdir = outdir
        self.nc_fn = nc_fn
        self.default_replica = default_replica
        self.default_properties = default_properties
        self.replica = {}
        self.dax_name = dax_name
        self.cart_files = cart_fn


    def generate_dax(self):
        "Calculate for nexrad files and run the composite workflow"
        if self.dax_name is None:
            ts = datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
            self.dax_name = "casa_hail_composite_wf-%s" % ts
        dax = ADAG(self.dax_name)
        dax.metadata("name", "CASA Hail Composite")
        USER = pwd.getpwuid(os.getuid())[0]
        dax.metadata("creator", "%s@%s" % (USER, os.uname()[1]))
        dax.metadata("created", time.ctime())

        for pfn in self.nc_fn:
            if pfn.startswith("/") or pfn.startswith("file://"):
                site = "local"
            else:
                site = pfn.split("/")[2]
            lfn = pfn.split("/")[-1]
            self.replica[lfn] = {"site": site, "pfn": pfn}

            if lfn.endswith(".gz"):
                radarfilename = lfn[:-3]
                unzip = Job("gunzip")
                unzip.addArguments(lfn)
                zipfile = File(lfn)
                unzip.uses(zipfile, link=Link.INPUT)
                unzip.uses(radarfilename, link=Link.OUTPUT, transfer=False, register=False)
                dax.addJob(unzip)
            else:
                radarfilename = lfn;

            string_end = lfn.find("-")
            file_time = lfn[string_end+1:string_end+16]
            file_ymd = file_time[0:8]
            file_hms = file_time[9:15]
            
            #print file_ymd
            #print file_hms
                
            radarfile = File(radarfilename)
            radarloc = lfn[0:string_end]

            radarconfigfilename = get_radar_config(radarloc)
            radarconfigfile = File(radarconfigfilename)
     
            soundingfilename = "current_sounding.txt"
            soundingfile = File(soundingfilename)
            
            hydroclass_outputcartfilename = radarloc + "-" + file_ymd + "-" + file_hms + ".hc.cart.netcdf"
            self.cart_files.append(hydroclass_outputcartfilename)
            
            hydroclass_outputcartfile = File(hydroclass_outputcartfilename)
            cart_out_files.append(hydroclass_outputcartfile)

            radx_configfilename = radarloc + "_latlon.txt"
            radx_configfile = File(radx_configfilename)
            
            # process burleson.tx data
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


        # prepare composite part
        cart_inputs = []
        
        for f in self.cart_files:
            string_end = f.find("-")
            file_time = f[string_end+1:string_end+16]
            cart_inputs.append([f, file_time])

        #sort based on time
        cart_inputs.sort(key = lambda x: x[1])
        file_time = cart_inputs[0][1]
        file_ymd = file_time[0:8]
        file_hms = file_time[9:15]
        #print file_time
        #print file_ymd
        #print file_hms
        
        cart_inputs = [x[0] for x in cart_inputs]

	inputtxtfilename = "composite_cart_input.txt"
        inputtxtfile = File(inputtxtfilename)
        with open(inputtxtfilename, "w+") as g:
            for f in cart_inputs:
                g.write(f)
                g.write("\n")

        composite_outputfilename = "COMPOSITE_" + file_time + ".nc"
        composite_outputfile = File(composite_outputfilename)
        
        composite_job = Job("hc_composite")
        composite_job.addArguments(inputtxtfilename)
        composite_job.addArguments(composite_outputfilename)
        composite_job.uses(inputtxtfile, link=Link.INPUT)
        for cartfile in cart_inputs:
		composite_job.uses(cartfile, link=Link.INPUT)
	
	composite_job.uses(composite_outputfile, link=Link.OUTPUT, transfer=False, register=False)
        dax.addJob(composite_job)

        netcdf2png_colorscalefilename = "standard_mergedhmc_nosnow.png"
        netcdf2png_colorscalefile = File(netcdf2png_colorscalefilename)
        netcdf2png_outputfilename = "hca_" + file_ymd + "-" + file_hms + ".png"
        netcdf2png_outputfile = File(netcdf2png_outputfilename)
        
        netcdf2png_job = Job("merged_hydroclass_netcdf2png")
        netcdf2png_job.addArguments("-z", "-1,6", "-c", netcdf2png_colorscalefilename, "-q", "245", "-o", netcdf2png_outputfilename)
        netcdf2png_job.addArguments(composite_outputfilename)
        netcdf2png_job.uses(netcdf2png_colorscalefile, link=Link.INPUT)
        netcdf2png_job.uses(composite_outputfile, link=Link.INPUT)
        netcdf2png_job.uses(netcdf2png_outputfile, link=Link.OUTPUT, transfer=True, register=False)
	dax.addJob(netcdf2png_job)

        hmt_configfilename = "options.cfg"
        hmt_configfile = File(hmt_configfilename)
        hmt_outputfile = File("hmt_HAIL_CASA_" + file_ymd + "-" + file_hms + ".geojson")
        hmt_job = Job("d3_hmt")
        hmt_job.addArguments("-c", hmt_configfilename)
        hmt_job.addArguments(composite_outputfilename)
        hmt_job.uses(hmt_configfile, link=Link.INPUT)
        hmt_job.uses(composite_outputfile, link=Link.INPUT)
        hmt_job.uses(hmt_outputfile, link=Link.OUTPUT, transfer=True, register=False)
        dax.addJob(hmt_job)


        # Write the DAX file
        dax_file = os.path.join(self.outdir, dax.name+".dax")
        dax.writeXMLFile(dax_file)
        
        # Write replica catalog
        replica_file =  os.path.join(self.outdir, dax.name+".rc.txt")
        shutil.copy(self.default_replica, replica_file)
        with open(replica_file, 'a') as g:
            for lfn in self.replica:
                g.write("{0}    {1}    site=\"{2}\"\n".format(lfn, self.replica[lfn]["pfn"], self.replica[lfn]["site"]))

        # Write properties file
        properties_file =  os.path.join(self.outdir, dax.name+".properties")
        shutil.copy(self.default_properties, properties_file)
        with open(properties_file, 'a') as g:
            g.write("pegasus.catalog.replica.file={0}\n".format(replica_file))

        print "{0} {1}".format(dax_file,properties_file)

    def generate_workflow(self):
        # Generate dax
        self.generate_dax()
        
if __name__ == '__main__':
    parser = ArgumentParser(description="Hail Composite Workflow")
    parser.add_argument("-n", "--name", metavar="DAX_NAME", default=None, type=str, help="Dax Name", required=False)
    parser.add_argument("-f", "--files", metavar="INPUT_FILE", type=str, nargs="+", help="Nexrad Filename", required=True)
    parser.add_argument("-cf", "--cartfiles", metavar="CART_FILE", type=str, nargs="+", help="Cart Filename", required=True)
    parser.add_argument("-r", "--replica", metavar="DEFAULT_REPLICA", type=str, help="Default Replica Catalog", required=True)
    parser.add_argument("-p", "--properties", metavar="DEFAULT_PROPERTIES", type=str, help="Default Pegasus Properties", required=True)
    parser.add_argument("-o", "--outdir", metavar="OUTPUT_LOCATION", type=str, help="DAX Directory", required=True)

    args = parser.parse_args()
    outdir = os.path.abspath(args.outdir)
    
    if not os.path.isdir(args.outdir):
        os.makedirs(outdir)

    if len(args.files) > 1:
        print "Expected one file as input. Processing only the first file"
        args.files = args.files[:1]

    workflow = composite_hail_workflow(args.name outdir, args.files, args.cartfiles, args.properties, args.replica)
    workflow.generate_workflow()
