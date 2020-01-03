# Test Belle job submission script

from b2utils.bellesub import get_mdst_list
import pytest

def test_file_number():
    """
    Check we get the correct number of files
    """

    ## Test cases for data
    # http://bweb3.cc.kek.jp/mdst.php?ex=69&rs=1&re=9999&skm=HadronBorJ&dt=Any&bl=caseB
    mdst_list = get_mdst_list(is_data = True, exp = 69)
    assert len(mdst_list) == 758

    # http://bweb3.cc.kek.jp/mdst.php?ex=45&rs=47&re=47&skm=HadronBorJ&dt=on_resonance&bl=caseB
    mdst_list = get_mdst_list(is_data = True, exp = 45, data_type = 'on_resonance', run_start = 47, run_end = 47)
    assert len(mdst_list) == 1 and \
        mdst_list[0] == '/group/belle/bdata_b/dstprod/dat/e000045/HadronBJ/0127/on_resonance/00/HadronBJ-e000045r000047-b20090127_0910.mdst'
    
    # http://bweb3.cc.kek.jp/mdst.php?ex=69&rs=1&re=9999&skm=HadronBorJ&dt=2S_scan&bl=caseB
    mdst_list = get_mdst_list(is_data = True, exp = 69, data_type = '2S_scan')
    assert len(mdst_list) == 0

    ## Test cases for MC
    # http://bweb3.cc.kek.jp/montecarlo.php?ex=65&rs=1&re=9999&ty=Any&dt=Any&bl=caseB&st=Any
    mdst_list = get_mdst_list(is_data = False, exp = 65, stream = 'Any')
    assert len(mdst_list) == 16984

    # http://bweb3.cc.kek.jp/montecarlo.php?ex=55&rs=1&re=100&ty=evtgen-charm&dt=on_resonance&bl=caseB&st=0
    mdst_list = get_mdst_list(is_data = False, exp = 55, stream = 0, event_type = 'evtgen-charm', data_type = 'on_resonance')
    assert len(mdst_list) == 50

def test_data_type():
    """
    Make sure ValueError is raised for wrong data types
    """
    with pytest.raises(ValueError):
        get_mdst_list(is_data = True, exp = 65, data_type = 'resonance')

def test_event_type():
    """
    Make sure ValueError is raised for wrong event types
    """
    with pytest.raises(ValueError):
        get_mdst_list(is_data = False, exp = 65, event_type = 'charm')
