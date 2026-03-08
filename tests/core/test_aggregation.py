from datetime import datetime, timedelta

import pytest

from core.aggregation import aggregate_data, get_interval_start


class TestGetIntervalStart:
    """Tests for get_interval_start."""

    @pytest.fixture()
    def ref_time(self):
        """Provide a reference time for interval calculations."""
        return datetime(2024, 1, 1, 0, 0, 0)

    def test_minutes_basic(self, ref_time):
        """Verify interval start calculation for basic minute intervals."""
        ts = ref_time + timedelta(minutes=7)
        result = get_interval_start(ts, ref_time, "minutes", 5)
        assert result == datetime(2024, 1, 1, 0, 5, 0)

    def test_minutes_exact_boundary(self, ref_time):
        """Verify interval start calculation when timestamp falls exactly on a minute boundary."""
        ts = ref_time + timedelta(minutes=10)
        result = get_interval_start(ts, ref_time, "minute", 5)
        assert result == datetime(2024, 1, 1, 0, 10, 0)

    def test_hours_basic(self, ref_time):
        """Verify interval start calculation for basic hour intervals."""
        ts = ref_time + timedelta(hours=3, minutes=30)
        result = get_interval_start(ts, ref_time, "hours", 2)
        assert result == datetime(2024, 1, 1, 2, 0, 0)

    def test_hours_exact_boundary(self, ref_time):
        """Verify interval start calculation when timestamp falls exactly on an hour boundary."""
        ts = ref_time + timedelta(hours=4)
        result = get_interval_start(ts, ref_time, "hour", 2)
        assert result == datetime(2024, 1, 1, 4, 0, 0)

    def test_days_basic(self, ref_time):
        """Verify interval start calculation for basic day intervals."""
        ts = ref_time + timedelta(days=5)
        result = get_interval_start(ts, ref_time, "days", 3)
        assert result == datetime(2024, 1, 4, 0, 0, 0)

    def test_weeks_basic(self, ref_time):
        """Verify interval start calculation for basic week intervals."""
        ts = ref_time + timedelta(weeks=3)
        result = get_interval_start(ts, ref_time, "weeks", 2)
        assert result.hour == 0
        assert result.minute == 0

    def test_months_basic(self, ref_time):
        """Verify interval start calculation for basic month intervals."""
        ts = datetime(2024, 4, 15)
        result = get_interval_start(ts, ref_time, "months", 3)
        assert result.day == 1

    def test_months_wrapping_to_previous_year(self, ref_time):
        """When month subtraction yields <=0, it wraps to the previous year."""
        del ref_time
        # ref_time is 2024-01-01; timestamp is 2024-01-15 with interval_value=3
        # month_offset = (0*12 + 1 - 1) % 3 = 0; new_month = 1 - 0 = 1 (no wrap)
        # To trigger wrap: need month_offset > month
        # Use ref_time in March, timestamp in February of next year with interval=6
        ref = datetime(2024, 3, 1)
        ts = datetime(2025, 2, 15)
        # month_offset = ((2025-2024)*12 + 2 - 3) % 6 = (12 + 2 - 3) % 6 = 11 % 6 = 5
        # new_month = 2 - 5 = -3 <= 0 → new_month = -3 + 12 = 9, year = 2025 - 1 = 2024
        result = get_interval_start(ts, ref, "months", 6)
        assert result.year == 2024
        assert result.month == 9
        assert result.day == 1
        assert result.hour == 0

    def test_unsupported_unit_raises(self, ref_time):
        """Verify that an unsupported interval unit raises a ValueError."""
        with pytest.raises(ValueError, match="Unsupported interval unit"):
            get_interval_start(ref_time, ref_time, "seconds", 1)


class TestAggregateData:
    """Tests for aggregate_data."""

    @pytest.fixture()
    def sample_data(self):
        """Provide sample data for aggregation tests."""
        base = datetime(2024, 1, 1, 0, 0, 0)
        return [
            {
                "timestamp": (base + timedelta(minutes=i)).isoformat(),
                "sensor": "s1",
                "value": float(i),
            }
            for i in range(10)
        ]

    def test_avg_aggregation(self, sample_data):
        """Verify that average aggregation computes correctly."""
        result = aggregate_data(sample_data, 5, "minutes", "avg", "value", "sensor")
        assert len(result) > 0
        assert any("avg_value" in r for r in result)

    def test_sum_aggregation(self, sample_data):
        """Verify that sum aggregation computes correctly."""
        result = aggregate_data(sample_data, 5, "minutes", "sum", "value", "sensor")
        assert any("sum_value" in r for r in result)

    def test_count_aggregation(self, sample_data):
        """Verify that count aggregation computes correctly."""
        result = aggregate_data(sample_data, 5, "minutes", "count", "value", "sensor")
        assert any("count_value" in r for r in result)

    def test_min_aggregation(self, sample_data):
        """Verify that min aggregation computes correctly."""
        result = aggregate_data(sample_data, 5, "minutes", "min", "value", "sensor")
        assert any("min_value" in r for r in result)

    def test_max_aggregation(self, sample_data):
        """Verify that max aggregation computes correctly."""
        result = aggregate_data(sample_data, 5, "minutes", "max", "value", "sensor")
        assert any("max_value" in r for r in result)

    def test_distinct_aggregation(self, sample_data):
        """Verify that distinct aggregation computes correctly."""
        result = aggregate_data(sample_data, 5, "minutes", "distinct", "value", "sensor")
        assert any("distinct_value" in r for r in result)

    def test_unsupported_stat_raises(self, sample_data):
        """Verify that an unsupported statistical operation raises a ValueError."""
        with pytest.raises(ValueError, match="Unsupported statistical operation"):
            aggregate_data(sample_data, 5, "minutes", "median", "value", "sensor")

    def test_missing_group_by_column_raises(self, sample_data):
        """Verify that a missing group-by column raises a KeyError."""
        with pytest.raises(KeyError, match="does not exist"):
            aggregate_data(sample_data, 5, "minutes", "avg", "value", "nonexistent")
