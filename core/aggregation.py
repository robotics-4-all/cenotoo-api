from datetime import timedelta

import numpy as np
import pandas as pd


def get_interval_start(timestamp, reference_time, interval_unit, interval_value=1):
    """Calculate the start time of an interval for a given timestamp."""
    if interval_unit.lower() in ["minutes", "minute"]:
        total_minutes_since_reference = int((timestamp - reference_time).total_seconds() // 60)
        interval_start_minutes = (total_minutes_since_reference // interval_value) * interval_value
        interval_start = reference_time + timedelta(minutes=interval_start_minutes)
        return interval_start.replace(second=0, microsecond=0)

    if interval_unit.lower() in ["hours", "hour"]:
        total_hours_since_reference = int((timestamp - reference_time).total_seconds() // 3600)
        interval_start_hours = (total_hours_since_reference // interval_value) * interval_value
        interval_start = reference_time + timedelta(hours=interval_start_hours)
        return interval_start.replace(minute=0, second=0, microsecond=0)

    if interval_unit.lower() in ["day", "days"]:
        start_date = reference_time.replace(hour=0, minute=0, second=0, microsecond=0)
        days_offset = (timestamp - start_date).days % interval_value
        interval_start = start_date + timedelta(days=(timestamp - start_date).days - days_offset)
        return interval_start

    if interval_unit.lower() in ["weeks", "week"]:
        start_of_week = reference_time - timedelta(days=reference_time.weekday())
        weeks_since_reference = (timestamp - start_of_week).days // 7
        interval_start_week = (weeks_since_reference // interval_value) * interval_value
        interval_start = start_of_week + timedelta(weeks=interval_start_week)
        return interval_start.replace(hour=0, minute=0, second=0, microsecond=0)

    if interval_unit.lower() in ["month", "months"]:
        month_offset = (
            (timestamp.year - reference_time.year) * 12 + timestamp.month - reference_time.month
        ) % interval_value
        new_month = timestamp.month - month_offset
        if new_month <= 0:
            new_month += 12
            return timestamp.replace(
                year=timestamp.year - 1,
                month=new_month,
                day=1,
                hour=0,
                minute=0,
                second=0,
                microsecond=0,
            )
        return timestamp.replace(month=new_month, day=1, hour=0, minute=0, second=0, microsecond=0)

    raise ValueError(f"Unsupported interval unit: {interval_unit}")


def aggregate_data(data, interval_value, interval_unit, stat, attribute, group_by):
    """Aggregate time-series data based on specified intervals and statistics."""
    df = pd.DataFrame(data)
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    if attribute in df.columns:
        df[attribute] = pd.to_numeric(df[attribute], errors="coerce")

    if group_by not in df.columns:
        raise KeyError(f"The column '{group_by}' does not exist in the data.")

    reference_time = df["timestamp"].min()

    df["interval_start"] = df.apply(
        lambda row: get_interval_start(
            row["timestamp"], reference_time, interval_unit, interval_value
        ),
        axis=1,
    )

    if "interval_start" not in df.columns:
        raise KeyError("The 'interval_start' column could not be created.")

    if stat == "distinct":
        grouped = df.groupby([group_by, "interval_start"])[attribute].nunique().reset_index()
        grouped.rename(columns={attribute: f"distinct_{attribute}"}, inplace=True)
        return grouped.to_dict(orient="records")

    agg_func = {
        "avg": "mean",
        "max": "max",
        "min": "min",
        "sum": "sum",
        "count": "count",
    }
    if stat not in agg_func:
        raise ValueError(f"Unsupported statistical operation: {stat}")

    grouped = (
        df.groupby([group_by, "interval_start"]).agg({attribute: agg_func[stat]}).reset_index()
    )
    grouped.rename(columns={attribute: f"{stat}_{attribute}"}, inplace=True)
    grouped[f"{stat}_{attribute}"] = grouped[f"{stat}_{attribute}"].round(3)

    grouped = grouped.replace([float("inf"), float("-inf"), np.inf, -np.inf, np.nan, pd.NA], None)
    return grouped.to_dict(orient="records")
