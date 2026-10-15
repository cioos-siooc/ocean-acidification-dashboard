import json

from extractSensorTimeseries import series_query, source_rank_sql


class FakeClient:
    def __init__(self, device_config):
        self.device_config = device_config

    def query(self, q):
        class R:
            result_rows = [(json.dumps(self.device_config),)]
        return R


CRIP = {"locationCode": "CRIP", "sources": {"temperature": [
    {"locationCode": "CRIP.C1", "deviceCategoryCode": "CTD", "sensorCategoryCode": "temperature"},
    {"locationCode": "CRIP", "deviceCategoryCode": "WETLABS_WQM", "sensorCategoryCode": "temperature"},
]}}


def test_rank_follows_catalog_order_and_puts_unlisted_last():
    rank = source_rank_sql(FakeClient(CRIP), "id", "temperature")
    assert rank == ("if(indexOf(['CRIP.C1/CTD', 'CRIP/WETLABS_WQM'], source) = 0, 3, "
                    "indexOf(['CRIP.C1/CTD', 'CRIP/WETLABS_WQM'], source))")


def test_single_source_needs_no_rank():
    assert source_rank_sql(FakeClient({}), "id", "temperature") == "0"
    assert source_rank_sql(FakeClient(CRIP), "id", "salinity") == "0"


def test_merged_query_takes_one_value_per_key():
    q = series_query(FakeClient(CRIP), "id", "temperature", None, "x = 1", keys="time", columns="time, value")
    assert "argMin(value, if(indexOf(" in q and "GROUP BY time" in q


def test_instrument_query_filters_to_that_source():
    q = series_query(FakeClient(CRIP), "id", "temperature", "CRIP.C1/CTD", "x = 1", keys="time", columns="time, value")
    assert "source = 'CRIP.C1/CTD'" in q and "argMin" not in q
