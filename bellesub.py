#!/usr/bin/env python3
#
# Belle job submission
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

import b2biiConversion as b2c

# Belle event types
BELLE_EVENT_TYPES = ['Any', 'evtgen-mixed', 'evtgen-charged', 'evtgen-charm', 'evtgen-uds']

# Belle data types
BELLE_DATA_TYPES = ['Any', 'on_resonance']

def get_mdst_list(is_data, exp, run_start = 1, run_end = 9999, 
                  event_type = 'Any', data_type = 'Any', 
                  belle_level = 'caseB', stream = 0, skim = 'HadronBorJ'):
    """
    Return Belle mdst file list on KEKCC based on experiment information.

    Parameters:
        is_data (bool)
        exp (int)
        run_start (int)
        run_end (int)
        event_type (str)
        data_type (str)
        belle_level (str)
        stream (str)
        skim (str)
    Returns:
        A list of mdst files on KEKCC.
    """
    # Check event type and data type
    if not event_type in BELLE_EVENT_TYPES:
        raise ValueError(f'Invalid event_type: {event_type}')
    if not data_type in BELLE_DATA_TYPES:
        raise ValueError(f'Invalid data_type: {data_type}')

    if not is_data:
        assert event_type in ['Any', 'evtgen-mixed', 'evtgen-charged', 'evtgen-charm', 'evtgen-uds']
        url =  f'http://bweb3.cc.kek.jp/montecarlo.php?ex={exp}&rs={run_start}&re={run_end}&ty={event_type}&dt={data_type}&bl={belle_level}&st={stream}'
    else:
        assert data_type in BELLE_DATA_TYPES
        url = f'http://bweb3.cc.kek.jp/mdst.php?ex={exp}&rs={run_start}&re={run_end}&skm={skim}&dt={data_type}&bl={belle_level}'
    logging.info(f'Getting mdst from {url}')
    mdst_list = b2c.parse_process_url(url)
    if len(mdst) >= 3:
        logging.info("The first three mdst files are:")
        for i in range(3):
            logging.info(f"{mdst_list[i]}")
    return mdst_list


def get_data(proc = 'proc9', exp = 8, datatype = 'Continuum', skim = 'hlt_hadron', filetype = 'mdst', verbose = 1):
    """
    Get list of root files for real data on KEK.
    """
    if proc == 'proc9':
        basedir = '/group/belle2/dataprod/Data/release-03-02-02/DB00000654/proc9'
        goodruns = glob.glob(os.path.join(basedir, "e%04d" % exp, datatype, 'GoodRuns', '*'))
        goodcount = len(goodruns)
        files = []
        for run in goodruns:
            files += glob.glob(os.path.join(run, 'skim', skim, filetype, 'sub00', '*.root'))
    else:
        raise Exception(f'Invalid data reprocessing {proc}')
    
    logging.info(f"proc = {proc} exp = {exp} datatype = {datatype} skim = {skim} filetype = {filetype}")
    logging.info(f'number of good runs = {len(goodruns)}')
    logging.info(f"number of root files in the list = {len(files)}")
    return files
    

def create_dir(path, clear = 'ask'):
    """
    Create an directory. If the given directory already exists then will clear this directory
    per user request.
    """
    logging.info(f'Creating dir: {path}')
    if os.path.isdir(path) and len(os.listdir(path)) > 0:
        if clear == 'ask':
            choice = input('Output dir not empty. Clear or not? [y/n] ').strip().lower()
        elif clear == 'yes':
            choice = 'y'
        elif clear == 'no':
            choice = 'n'
        else:
            choice = 'n'
            logging.warning('Invalid choice. Will not clear the dir.')
        
        if choice == '' or choice.startswith('y'):
            logging.info(f'Clearing the contents of {path}')
            shutil.rmtree(path)
    os.makedirs(path, exist_ok = True)

def create_jobs(outdir, script, infiles, q = 's', b2opt = ''):
    """
    Create bsub job submission cmd lines based on input files.
    
    Returns:
        A list of bsubs commands to be ran (using multiprocessing)
    """
    cmdlist = []
    for infile in infiles:
        base = os.path.basename(infile)
        logfile = os.path.join(outdir, base + '.log')
        outfile = os.path.join(outdir, 'ntuple.' + base)
        cmdlist += [f'bsub -q s -oo {logfile} basf2 {b2opt} {script} {infile} {outfile} >> bsub.log']

    logging.info(f"{len(cmdlist)} jobs created")
    logging.info(f"The first job: {cmdlist[0]}")
    return cmdlist

def fake_system(*args, **kwargs):def create_dir(path, clear = 'ask'):
    """
    Create an directory. If the given directory already exists then will clear this directory
    per user request.
    """
    logging.info(f'Creating dir: {path}')
    if os.path.isdir(path) and len(os.listdir(path)) > 0:
        if clear == 'ask':
            choice = input('Output dir not empty. Clear or not? [y/n] ').strip().lower()
        elif clear == 'yes':
            choice = 'y'
        elif clear == 'no':
            choice = 'n'
        else:
            choice = 'n'
            logging.warning('Invalid choice. Will not clear the dir.')
        
        if choice == '' or choice.startswith('y'):
            logging.info(f'Clearing the contents of {path}')
            shutil.rmtree(path)
    os.makedirs(path, exist_ok = True)
    """
    A fake os.system to test multiprocessing job submission.
    """
    time.sleep(1)
    return 0

def submit_jobs(cmdlist, nworkers = 8):
    """
    Submit all jobs in cmdlist in parallel using multiprocessing
    """
    pool = mpl.Pool(processes = nworkers)
    
    logging.info(f"{len(cmdlist)} jobs to submit")
    logging.info(f'Started to submit jobs with {nworkers} workers')
    
    bar = tqdm(total = len(cmdlist))
    results = []

    def log_result(result):
        results.append(result)
        bar.update()
    
    for cmd in cmdlist:
        pool.apply_async(os.system, [cmd], callback = log_result)
    pool.close()
    pool.join()
    
    logging.debug('Checking for failed jobs submission...')
    assert len(results) == len(cmdlist)
    failed = []
    for i in range(len(results)):
        if results[i] != 0:
            failed += cmdlist[i]
    if len(failed) == 0:
        logging.info(colored(f"{len(failed)} failed submission", 'green'))
    else:
        logging.info(colored(f"{len(failed)} failed submission", 'red'))
    if len(failed) > 0:
        logging.warning(f"The first failed submission: {failed[0]}")
    
def submit_lambda():
    print("lambda exp 8 continuum")
    outdir = 'lambda_exp8'
    script = 'test_lambdas.py'
    
    create_dir(outdir, clear = 'ask')
    # exp 8 continuum and 4S
    files_cont = get_data(proc = 'proc9', exp = 8, datatype = 'Continuum')
    files_cont = files_cont[:100]
    cmds = create_jobs(outdir, script, files_cont)
    submit_jobs(cmds, nworkers = 10)

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
    
    logging.basicConfig(format = '[%(levelname)s] %(funcName)s: %(message)s', level = logging.DEBUG, stream = sys.stdout)

    submit_lambda()