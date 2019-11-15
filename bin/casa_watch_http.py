#!/usr/bin/env python3

import urllib.parse as parse
import urllib.request as request
import urllib.error as error
from datetime import datetime
from argparse import ArgumentParser
from configparser import ConfigParser
from multiprocessing import Process, Value, Lock
import threading
import subprocess
import signal
import time
import re
import os
import sys

#### globals ####
re_file = "<a href=\"(.*)\">.*</a>"
re_date = "<td align=\"right\">([0-9]{4}-(0[1-9]|1[0-2])-(0[1-9]|[1-2][0-9]|3[0-1])\s[0-9:]*)\s*</td>"
re_size = "<td align=\"right\">\s*([0-9.]*?[KM]?)\s*</td>"
datetime_string = "%Y-%m-%d %H:%M"
poll_interval = 2
signal_raised = Value('i', 0)
########

def worker_signal_handler(signum, frame):
    print("Worker received signal.")
    return


def parent_signal_handler(signum, frame):
    global signal_raised
    print("Parent received signal. Exiting gracefully...")
    signal_raised.value = 1
    return


def retrieve_new_files(lock, url, accepted_prefixes, accepted_suffixes, file_cache, new_files, new_files_ts, last_operated_ts):
    user_agent = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.120 Safari/537.36"
    headers = {"User-Agent": user_agent}
    
    #add GET parameters to sort based on the last updated field
    url_get = url + "?C=M;O=D"
    
    req = request.Request(url_get, headers=headers)
    try:
        with request.urlopen(req) as response:
            the_page = response.read().decode('utf-8')
            table_start = the_page.find("<table>")
            table_end = the_page.find("</table>")
            records = the_page[table_start+len("<table>"):table_end].splitlines()[4:-1]
            for line in records:
                file_name = re.search(re_file, line).group(1)
                file_last_edited = int(datetime.strptime(re.search(re_date, line).group(1), datetime_string).timestamp())
                file_size = re.search(re_size, line).group(1)

                #check file suffix
                pfx_sfx_guard = True
                for sfx in accepted_suffixes:
                    if file_name.endswith(sfx):
                        pfx_sfx_guard = False
                        break

                #check file prefix
                for pfx in accepted_prefixes:
                    if file_name.startswith(pfx):
                        pfx_sfx_guard = False
                        break

                if pfx_sfx_guard:
                    continue
            
                #the lock needs to be placed better
                lock.acquire()
                if file_name in file_cache:
                    if file_cache[file_name]["last_edited"] == file_last_edited:
                        lock.release()
                        break #files are sorted based on last edit
                    elif file_cache[file_name]["last_edited"] < file_last_edited:
                        file_cache[file_name]["last_edited"] = file_last_edited
                        file_cache[file_name]["size"] = file_size
                        new_files.add(file_name)
                    else:
                        print("Unexpected state: Last recorded update time is in the past.")
                else:
                    file_cache[file_name] = {"last_edited": file_last_edited, "size": file_size, "href": url + file_name}
                    string_start = file_name.find("-")
                    string_end = file_name.find(".", string_start)
                    file_time = file_name[string_start+1:string_end-2]
                    if file_time < last_operated_ts:
                        print(f"Unexpected state: A new file ({file_name}) arrived really late. Adding to the cache but not the new files to operate on.")
                    else:
                        new_files.add(file_name)
                        if not file_time in new_files_ts:
                            new_files_ts.add(file_time)
                lock.release()
    except error.URLError as e:
        print(e.reason)

    return (new_files, new_files_ts)


def casa_watch_worker(signal_raised, workflow_type, url_list, accepted_prefixes, accepted_suffixes, workflow_dir, trigger_script, interval):
    signal.signal(signal.SIGINT, worker_signal_handler)
    signal.signal(signal.SIGTERM, worker_signal_handler)
    
    file_cache = {}
    new_files = set()
    new_files_ts = set()
    last_operated_ts = "0"
    lock = Lock()

    for k in range(len(url_list)):
        if not url_list[k][-1] == "/":
            url_list[k] += "/"
        print(f"Watch Worker - {workflow_type}: Watching {url_list[k]} and submitting workflows every {interval} seconds using {workflow_dir}.")
    
    start_time = time.time()
    while signal_raised.value == 0:
        threadArray = []
        for url in url_list:
            thread = threading.Thread(name="#{workflow_type}-{url}", target=retrieve_new_files, args=(lock, url, accepted_prefixes, accepted_suffixes, file_cache, new_files, new_files_ts, last_operated_ts))
            threadArray.append(thread)
            thread.start()
        
        for thread in threadArray:
            thread.join()


        current_time = time.time()
        if workflow_type in ["generic", "nowcast", "hail_composite", "wind"] and (current_time - start_time) > interval:
            start_time = current_time
            if workflow_type in ["nowcast", "hail_composite", "generic"]:
                for f in new_files:
                    command = [os.path.join(workflow_dir, trigger_script), file_cache[f]["href"]]
                    try:
                        print(f"Watch Worker - {workflow_type}: Triggering Workflow.")
                        pegasus_trigger = subprocess.run(command, cwd=workflow_dir, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=False, check=True)
                    except OSError as e:
                        print(f"Watch Worker - {workflow_type}: Failed to start workflow with error code {e.errno} -> {e.strerror}.")
                    except subprocess.SubprocessError as e:
                        print(f"Watch Worker - {workflow_type}: Failed to start workflow with error code {e.returncode} -> {e.stderr}.")
            elif workflow_type == "wind":
                command = [os.path.join(workflow_dir, trigger_script)]
                for f in new_files:
                    command.append(file_cache[f]["href"])
                try:
                    print(f"Watch Worker - {workflow_type}: Triggering Workflow.")
                    pegasus_trigger = subprocess.run(command, cwd=workflow_dir, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=False, check=True)
                except OSError as e:
                    print(f"Watch Worker - {workflow_type}: Failed to start workflow with error code {e.errno} -> {e.strerror}.")
                except subprocess.SubprocessError as e:
                    print(f"Watch Worker - {workflow_type}: Failed to start workflow with error code {e.returncode} -> {e.stderr}.")

            new_files = set()
            new_files_ts = set()

        elif workflow_type == "hail" and (current_time - start_time) > interval and len(new_files_ts) > 1:
            start_time = current_time
            
            candidate_ts = min(new_files_ts)
            candidate_files = [f for f in new_files if candidate_ts in f]
            
            command = [os.path.join(workflow_dir, trigger_script)]
            for f in candidate_files:
                command.append(file_cache[f]["href"])
            try:
                print(f"Watch Worker - {workflow_type}: Triggering Workflow.")
                pegasus_trigger = subprocess.run(command, cwd=workflow_dir, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=False, check=True)
            except OSError as e:
                print(f"Watch Worker - {workflow_type}: Failed to start workflow with error code {e.errno} -> {e.strerror}.")
            except subprocess.SubprocessError as e:
                print(f"Watch Worker - {workflow_type}: Failed to start workflow with error code {e.returncode} -> {e.stderr}.")
            
            for f in candidate_files:
                new_files.remove(f)
            new_files_ts.remove(candidate_ts)
            last_operated_ts = candidate_ts

        # TODO: Delete things from cache to keep it less than 10MB
        #if sys.getsizeof(file_cache) >= 10485760:
        #    print("Size of file_cache is: {0}".format(sys.getsizeof(file_cache)))

        time.sleep(poll_interval)

    return


if __name__ == "__main__":
    signal.signal(signal.SIGINT, parent_signal_handler)
    signal.signal(signal.SIGTERM, parent_signal_handler)

    config = ConfigParser()
    parser = ArgumentParser(description="Watch HTTP repository for changes in CASA files")
    parser.add_argument("-c", "--conf", metavar="STR", type=str, help="HTTP watch configuration file", required=True)
    parser.add_argument("-t", "--timeout", metavar="INT", type=int, default=0, help="HTTP watch timeout in seconds", required=False)

    args = parser.parse_args()

    if args.conf:
        config.read_file(open(args.conf))

    processArray = []
    for section in config.sections():
        #checks start
        if "url_list" in config[section]:
            url_list = config[section]["url_list"].strip(",").replace(" ", "").split(",")
            if url_list == []:
                print(f"In {section} $url_list was empty. Skipping...")
                continue
        else:
            print(f"In {section} $url_list is missing. Skipping...")
            continue
        
        if "workflow_dir" in config[section]:
            workflow_dir = config[section]["workflow_dir"].replace(" ", "")
            if workflow_dir == "":
                print(f"In {section} $workflow_dir was empty. Skipping...")
                continue
            else:
                workflow_dir = os.path.expandvars(workflow_dir)
        else:
            print(f"In {section} $workflow_dir is missing. Skipping...")
            continue
    
        if "trigger_script" in config[section]:
            trigger_script = config[section]["trigger_script"].replace(" ", "")
            if trigger_script == "":
                print(f"In {section} $trigger_script was empty. Skipping...")
                continue
        else:
            print(f"In {section} $trigger_script is missing. Skipping...")
            continue
        
        if "accepted_prefixes" in config[section]:
            accepted_prefixes = config[section]["accepted_prefixes"].strip(",").replace(" ", "").split(",")
        else:
            accepted_prefixes = []

        if "accepted_suffixes" in config[section]:
            accepted_suffixes = config[section]["accepted_suffixes"].strip(",").replace(" ", "").split(",")
        else:
            accepted_suffixes = []

        if accepted_prefixes == [] and accepted_suffixes == []:
            print(f"In {section} no prefixes or suffixes were specified. Defaults to suffix -> ['.netcdf', '.netcdf.gz'].")
        #checks end
        
        if config[section]["workflow_type"] == "nowcast":
            p = Process(target=casa_watch_worker, args=(signal_raised, "nowcast", url_list, accepted_prefixes, accepted_suffixes, workflow_dir, trigger_script, config.getint(section, "interval")))
            processArray.append(p)
            p.start()
        elif config[section]["workflow_type"] == "wind":
            p = Process(target=casa_watch_worker, args=(signal_raised, "wind", url_list, accepted_prefixes, accepted_suffixes, workflow_dir, trigger_script, config.getint(section, "interval")))
            processArray.append(p)
            p.start()
        elif config[section]["workflow_type"] == "hail":
            p = Process(target=casa_watch_worker, args=(signal_raised, "hail", url_list, accepted_prefixes, accepted_suffixes, workflow_dir, trigger_script, config.getint(section, "interval")))
            processArray.append(p)
            p.start()
        elif config[section]["workflow_type"] == "hail_composite":
            p = Process(target=casa_watch_worker, args=(signal_raised, "hail_composite", url_list, accepted_prefixes, accepted_suffixes, workflow_dir, trigger_script, config.getint(section, "interval")))
            processArray.append(p)
            p.start()
        elif config[section]["workflow_type"] == "generic":
            p = Process(target=casa_watch_worker, args=(signal_raised, "generic", url_list, accepted_prefixes, accepted_suffixes, workflow_dir, trigger_script, config.getint(section, "interval")))
            processArray.append(p)
            p.start()
        else:
            print("Parser: Unknown workflow type. Skipping...")


    print("Parser: Waiting for processes.")
    if args.timeout > 0:
        time.sleep(args.timeout)
        signal_raised.value = 1

    for p in processArray:
        p.join()

