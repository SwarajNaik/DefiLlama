import pytest
import sys
import os
from pathlib import Path

sys.path.insert(0, '/Users/swarajnaik/Desktop/DefiLlama/src')

import modular_data_pipeline as dp



@pytest.mark.asyncio
@pytest.fixture
async def fixture_test_call_data():
    processor = dp.DefiLlama()
    result = await processor._call_data(url = "https://yields.llama.fi/pools")
    return result

@pytest.mark.asyncio
@pytest.fixture
async def fixture_test_clean_up_pools(fixture_test_call_data):
    processor = dp.DefiLlama()
    json_data = await fixture_test_call_data 
    result = await processor._clean_up_pools(json_data['data'])
    return result


@pytest.mark.asyncio
@pytest.fixture
async def fixture_test_get_pool_id(fixture_test_clean_up_pools):
    processor = dp.DefiLlama()
    pools_data = await fixture_test_clean_up_pools
    result = await processor._get_pool_id(pools_data)
    return result



@pytest.mark.asyncio
async def test_call_data(fixture_test_call_data):
    data = await fixture_test_call_data
    assert len(data) > 0

@pytest.mark.asyncio
async def test_clean_up_pools(fixture_test_clean_up_pools):
    pools = await fixture_test_clean_up_pools
    assert len(pools) > 0

@pytest.mark.asyncio
async def test_get_pool_id(fixture_test_get_pool_id):
    pool_id = await fixture_test_get_pool_id
    assert len(pool_id) > 0

@pytest.mark.asyncio
async def test_get_pools_data(fixture_test_get_pool_id):
    processor = dp.DefiLlama()
    list_of_pools =  await fixture_test_get_pool_id# Add example data for testing
    result = await processor._get_pools_data(list_of_pools)
    assert len(result) > 0

@pytest.mark.asyncio
async def test_process_data():
    processor = dp.DefiLlama()
    os.chdir("/Users/swarajnaik/Desktop/DefiLlama/pytesting")
    process = await processor.process_data()
    assert process == "Done"
    

