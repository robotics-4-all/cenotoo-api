"""Flink job data models."""

import uuid
from typing import Literal

from pydantic import BaseModel, Field, model_validator

WindowType = Literal["tumbling", "sliding"]
TimeUnit = Literal["second", "minute", "hour", "day"]
Metric = Literal["avg", "min", "max", "sum", "count", "stddev"]
JobStatus = Literal["RUNNING", "CANCELLED", "ERROR", "PENDING"]


class GuidedJobRequest(BaseModel):
    """Request model for creating a guided Flink aggregation job."""

    attribute: str
    metric: Metric
    window_type: WindowType = "tumbling"
    window_size: int = Field(gt=0)
    unit: TimeUnit = "minute"
    sliding_step: int | None = Field(default=None, gt=0)

    @model_validator(mode="after")
    def sliding_step_required_for_sliding(self) -> "GuidedJobRequest":
        if self.window_type == "sliding" and self.sliding_step is None:
            raise ValueError("sliding_step is required when window_type is 'sliding'")
        return self


class FlinkJobResponse(BaseModel):
    """Response model for a Flink job."""

    id: uuid.UUID
    collection_id: uuid.UUID
    project_id: uuid.UUID
    job_type: str
    config: dict
    sink_topic: str
    status: JobStatus
    created_at: str


class FlinkJobListResponse(BaseModel):
    """Response model for a list of Flink jobs."""

    items: list[FlinkJobResponse]
    total: int


class FlinkJobResult(BaseModel):
    """A single window result from a Flink aggregation job."""

    key: str
    window_start: str
    window_end: str
    record_count: int
    value: float | None


class FlinkJobResultsResponse(BaseModel):
    """Response model for Flink job window results."""

    items: list[FlinkJobResult]
    total: int
    metric: str
    attribute: str
    sink_topic: str
