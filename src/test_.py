import pytest
from data_pipline import *

def test_call_data():
    headers = {"Content-Type": "application/json"}
    
    assert call_data("https://yields.llama.fi/pools", headers)

    

def test_clean_up_pools():
    headers = {"Content-Type": "application/json"}
    # json = call_data("https://yields.llama.fi/pools", headers)
    
    assert clean_up_pools(json=test_call_data())

if __name__=="__main__":
    pytest.main()
#     # test_call_data(url="https://yields.llama.fi/pools", headers=headers)

