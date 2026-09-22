"""Open interest: REST and Vision parse to the same rows."""

from __future__ import annotations

from datetime import UTC, datetime

from binance_fetcher.pipeline.update import HOUR_MS
from binance_fetcher.transform.open_interest import (
    parse_api_open_interest,
    parse_vision_metrics_csv,
)

T0 = int(datetime(2023, 11, 14, 0, tzinfo=UTC).timestamp() * 1000)


def test_rest_and_vision_parse_to_identical_rows():
    rest = parse_api_open_interest(
        [
            {"timestamp": T0, "sumOpenInterest": "10", "sumOpenInterestValue": "100"},
            {"timestamp": T0 + HOUR_MS, "sumOpenInterest": "11.5", "sumOpenInterestValue": "115"},
        ]
    )
    vision = parse_vision_metrics_csv(
        b"create_time,symbol,sum_open_interest,sum_open_interest_value,a,b,c,d\n"
        b"2023-11-14 00:00:00,BTCUSDT,10,100,1,1,1,1\n"
        b"2023-11-14 00:05:00,BTCUSDT,99,999,1,1,1,1\n"  # not an hour boundary: dropped
        b"2023-11-14 01:00:00,BTCUSDT,11.5,115,1,1,1,1\n"
    )
    assert rest.equals(vision)
    assert rest["timestamp"].to_list() == [T0, T0 + HOUR_MS]


def test_parsers_handle_empty_input():
    assert len(parse_api_open_interest([])) == 0
    assert len(parse_vision_metrics_csv(b"")) == 0
