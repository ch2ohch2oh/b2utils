#!/usr/bin/env python3
#
# Belle job commands
#
import os
import sys
import glob
from tqdm import tqdm
import logging
import multiprocessing as mpl
import time
import shutil
from termcolor import colored
import argparse

import b2biiConversion as b2c

# Belle event types
BELLE_EVENT_TYPES = ['Any', 'evtgen-mixed', 'evtgen-charged', 'evtgen-charm', 'evtgen-uds']

# Belle data types
BELLE_DATA_TYPES = ['Any', 'on_resonance', 'continuum', '5S_scan', '5S_onresonance', 
    '1S_scan', '2S_scan', '3S_scan']

def get_mdst_list(is_data, exp, run_start = 1, run_end = 9999, 
                  event_type = 'Any', data_type = 'Any', 
                  belle_level = 'caseB', stream = 0, skim = 'HadronBorJ'):
    """
    Return Belle mdst file list on KEKCC based on experiment information.

    Parameters:
        is_data (bool): data or MC
        exp (int): exp number
        run_start (int): run number start
        run_end (int): run number end
        event_type (str): event type
        data_type (str): data type
        belle_level (str): Belle level
        stream (str): MC stream number
        skim (str): skim type
    Returns:
        A list of mdst files on KEKCC.
    """
    # Check event type and data type
    if not event_type in BELLE_EVENT_TYPES:
        raise ValueError(f'Invalid event_type: {event_type}')
    if not data_type in BELLE_DATA_TYPES:
        raise ValueError(f'Invalid data_type: {data_type}')

    if not is_data:
        url =  f'http://bweb3.cc.kek.jp/montecarlo.php?ex={exp}&rs={run_start}&re={run_end}&ty={event_type}&dt={data_type}&bl={belle_level}&st={stream}'
    else:
        url = f'http://bweb3.cc.kek.jp/mdst.php?ex={exp}&rs={run_start}&re={run_end}&skm={skim}&dt={data_type}&bl={belle_level}'
    logging.info(f'Getting mdst from {url}')
    mdst_list = b2c.parse_process_url(url)
    if len(mdst_list) >= 3:
        logging.info("The first three mdst files are:")
        for i in range(3):
            logging.info(f"{mdst_list[i]}")
    return mdst_list    

def create_dir(path, clear = None):
    """
    Create an directory. If the given directory already exists then will clear this directory
    per user request.

    Parameters:
        path (str): path to create
        clear (bool): clear the path or not if the path already exists.
            True -> clear the path
            False -> do no clear the path
            None -> ask the user
    """
    logging.debug(f'Creating dir: {path}')
    if os.path.isdir(path) and len(os.listdir(path)) > 0:
        if clear == None:
            print(f"Path {path} not empty")
            clear = input('Clear the path or not? [y/n] ').strip().lower().startswith('y')
        if clear == True:
            logging.info(f'Clearing the contents of {path}')
            shutil.rmtree(path)
    os.makedirs(path, exist_ok = True)
    # Check again the output dir exists
    if not os.path.exists(path):
        raise FileNotFoundError(f"Path {path} does not exist!")

def create_bsub_cmds(outdir, script, infiles, q = 's', b2opt = ''):
    """
    Create bsub job commands cmd lines based on input files.
    
    Parameters:
        outdir (str): output dir
        script (str): steering script 
        infiles (list): input mdst file list
        q (str): bsub queue (default = 's')
        b2opt (str): basf2 options (default = '')
    Returns:
        A list of  commands
    """
    # Check file exists
    if not os.path.exists(outdir):
        raise FileNotFoundError('outdir ({outdir}) does not exist!')
    if not os.path.exists(script):
        raise FileNotFoundError('script ({script}) does not exist!')
    
    cmdlist = []
    for infile in infiles:
        base = os.path.basename(infile)
        logfile = os.path.join(outdir, base + '.log')
        outfile = os.path.join(outdir, 'ntuple.' + base)
        cmdlist += [f'bsub -q s -oo {logfile} basf2 {b2opt} {script} {infile} {outfile} >> bsub.log']

    logging.info(f"{len(cmdlist)} bsub commands created")
    logging.info(f"The first bsub command: {cmdlist[0]}")
    return cmdlist

def run_cmds(cmdlist, nworkers = 8):
    """
    Run commands in parallel using multiprocessing.
    
    Parameters:
        cmdlist (list) list of commands
        nworkers (int) number of workers to run the commans
    Returns:
        Number of failed commands
    """
    pool = mpl.Pool(processes = nworkers)
    
    logging.info(f"{len(cmdlist)} commands to run")
    logging.info(f'nworkers = {nworkers}')
    
    bar = tqdm(total = len(cmdlist))
    results = []

    def log_result(result):
        results.append(result)
        bar.update()
    
    for cmd in cmdlist:
        pool.apply_async(os.system, [cmd], callback = log_result)
    pool.close()
    pool.join()
    
    logging.debug('Checking for failed commands...')
    assert len(results) == len(cmdlist)
    failed = []
    for i in range(len(results)):
        if results[i] != 0:
            failed += cmdlist[i]
    
    if len(failed) == 0:
        logging.info(colored("No failed commands", 'green'))
    else:
        logging.info(colored(f"{len(failed)} failed commands", 'red'))
    if len(failed) > 0:
        logging.warning(f"The first failed commands: {failed[0]}")
    return len(failed)

def parse_arguments():
    """
    Parse command line arguments

    Returns:
        argparse parser.
    """
    parser = argparse.ArgumentParser(formatter_class = argparse.ArgumentDefaultsHelpFormatter)
    
    parser.add_argument('script', help = 'steering script to run')
    parser.add_argument('outdir', help = 'output dir')
    
    parser.add_argument('--is_data', action = 'store_true', default = False, help = 'MC or data')
    parser.add_argument('--exp', type=int, default = 65, help = 'exp no.')
    parser.add_argument('--run_start', type = int, help = 'run number start', default = 1)
    parser.add_argument('--run_end', type = int, help = 'run number end', default = 9999)
    parser.add_argument('--event_type', help = 'event type', 
                        default = 'Any', choices = BELLE_EVENT_TYPES)
    parser.add_argument('--data_type', help = 'data type', 
                        default = 'on_resonance', choices = BELLE_DATA_TYPES)
    parser.add_argument('--skim', help = 'skim type', 
                        default = 'HadronBorJ')
    parser.add_argument('--stream',  type = int, default = 0,
                        help = 'stream number (for MC)')
    
    parser.add_argument('--one', action = 'store_true', default = False,
                        help = 'only process the first mdst in the list')
    parser.add_argument('--clear', action = 'store_true', default = False,
                        help = 'clear output dir or not')
    if len(sys.argv) == 1:
        parser.print_help(sys.stderr)
        sys.exit(1)
    args = parser.parse_args()
    return args

# Set up logger for the module
logging.basicConfig(format = '[%(levelname)s] %(funcName)s: %(message)s', level = logging.DEBUG, stream = sys.stdout)

if __name__ == '__main__':
    args = parse_arguments()

    create_dir(args.outdir)
    
    mdst_list = get_mdst_list(
        is_data = args.is_data,
        exp = args.exp,
        run_start = args.run_start,
        run_end = args.run_end,
        event_type = args.event_type,
        data_type = args.data_type,
        belle_level = args.belle_level,
        stream = args.stream,
        skim = args.skim
    )
    
    if len(mdst_list) == 0:
        logging.warning('Zero mdst found for specified experiment info.')
        sys.exit(1)
    
    cmds = create_bsub_cmds(args.outdir, args.script, mdst_list)
    failed_count = run_cmds(cmds)
    