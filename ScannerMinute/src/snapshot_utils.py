from collections import OrderedDict
from datetime import datetime

import pytz

time_template = "%Y-%m-%d %H:%M:%S"


class Snapshot(OrderedDict):

    def __init__(self, item=None, time_rounded=None):
        self["time_rounded"] = time_rounded
        self["ticker"] = (
            item.ticker if hasattr(item, "ticker") and item.ticker is not None else None
        )
        self["today_change"] = (
            item.todays_change
            if hasattr(item, "todays_change") and item.todays_change is not None
            else None
        )
        self["today_change_percent"] = (
            item.todays_change_percent
            if hasattr(item, "todays_change_percent")
            and item.todays_change_percent is not None
            else None
        )
        self["updated"] = (
            item.updated
            if hasattr(item, "updated") and item.updated is not None
            else None
        )
        self["updated_human_utc"] = (
            datetime.fromtimestamp(item.updated / 1000000000, tz=pytz.UTC).strftime(
                time_template
            )
            if hasattr(item, "updated") and item.updated is not None
            else None
        )
        # -----
        pd = getattr(item, "prev_day", None)
        self["prev_day.open"] = (
            pd.open if pd and hasattr(pd, "open") and pd.open is not None else None
        )
        self["prev_day.high"] = (
            pd.high if pd and hasattr(pd, "high") and pd.high is not None else None
        )
        self["prev_day.low"] = (
            pd.low if pd and hasattr(pd, "low") and pd.low is not None else None
        )
        self["prev_day.close"] = (
            pd.close if pd and hasattr(pd, "close") and pd.close is not None else None
        )
        self["prev_day.volume"] = (
            pd.volume
            if pd and hasattr(pd, "volume") and pd.volume is not None
            else None
        )
        self["prev_day.vwap"] = (
            pd.vwap if pd and hasattr(pd, "vwap") and pd.vwap is not None else None
        )
        self["prev_day.timestamp"] = (
            pd.timestamp
            if pd and hasattr(pd, "timestamp") and pd.timestamp is not None
            else None
        )
        self["prev_day.timestamp_human_utc"] = (
            datetime.fromtimestamp(pd.timestamp / 1000000000, tz=pytz.UTC).strftime(
                time_template
            )
            if pd and hasattr(pd, "timestamp") and pd.timestamp is not None
            else None
        )
        self["prev_day.transactions"] = (
            pd.transactions
            if pd and hasattr(pd, "transactions") and pd.transactions is not None
            else None
        )
        self["prev_day.otc"] = (
            pd.otc if pd and hasattr(pd, "otc") and pd.otc is not None else None
        )
        # -----
        mn = getattr(item, "min", None)
        # minute = min
        self["min.accumulated_volume"] = (
            mn.accumulated_volume
            if mn
            and hasattr(mn, "accumulated_volume")
            and mn.accumulated_volume is not None
            else None
        )
        self["min.open"] = (
            mn.open if mn and hasattr(mn, "open") and mn.open is not None else None
        )
        self["min.high"] = (
            mn.high if mn and hasattr(mn, "high") and mn.high is not None else None
        )
        self["min.low"] = (
            mn.low if mn and hasattr(mn, "low") and mn.low is not None else None
        )
        self["min.close"] = (
            mn.close if mn and hasattr(mn, "close") and mn.close is not None else None
        )
        self["min.volume"] = (
            mn.volume
            if mn and hasattr(mn, "volume") and mn.volume is not None
            else None
        )
        self["min.vwap"] = (
            mn.vwap if mn and hasattr(mn, "vwap") and mn.vwap is not None else None
        )
        self["min.otc"] = (
            mn.otc if mn and hasattr(mn, "otc") and mn.otc is not None else None
        )
        self["min.timestamp"] = (
            mn.timestamp
            if mn and hasattr(mn, "timestamp") and mn.timestamp is not None
            else None
        )
        self["min.timestamp_human_utc"] = (
            datetime.fromtimestamp(mn.timestamp / 1000, tz=pytz.UTC).strftime(
                time_template
            )
            if mn and hasattr(mn, "timestamp") and mn.timestamp is not None
            else None
        )
        # -----
        self["last_quote"] = (
            item.last_quote
            if hasattr(item, "last_quote") and item.last_quote is not None
            else None
        )
        self["last_trade"] = (
            item.last_trade
            if hasattr(item, "last_trade") and item.last_trade is not None
            else None
        )

    def __str__(self, sep="\n"):
        """Human-readable string; sep is the separator between lines (default newline)."""
        lines = [f"  {key}: {value}" for key, value in self.items()]
        return sep.join(lines)

    def __repr__(self):
        """Unambiguous representation for repr() and debugger."""
        return f"Snapshot({dict(self)})"
