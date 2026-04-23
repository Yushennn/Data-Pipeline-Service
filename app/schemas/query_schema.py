from enum import Enum
from typing import List, Union

from pydantic import BaseModel, Field, field_validator, model_validator


ScalarFilterValue = Union[str, int, float, bool]
FilterValue = Union[
    str,
    int,
    float,
    bool,
    List[str],
    List[int],
    List[float],
    List[bool],
]

class TargetTableEnum(str, Enum):
    match_performance = "MatchPerformance"
    set_performance = "SetPerformance"


class FilterTableEnum(str, Enum):
    athlete = "Athlete"
    match = "Match"
    match_performance = "MatchPerformance"
    set_performance = "SetPerformance"
    
class OperationEnum(str, Enum):
    eq = "eq" # equal (=)
    neq = "neq" # not equal (!=)
    gt = "gt" # greater than (>)
    gte = "gte" # greater than or equal (>=)
    lt = "lt" # less than (<)
    lte = "lte" # less than or equal (<=)
    contains = "contains" # contains (for strings, lists, etc.)
    in_ = "in" # in (for checking if a value is in a list of values)


class FilterCondition(BaseModel):
    table: FilterTableEnum = Field(
        ...,
        description="The joined table that owns the column being filtered"
    )
    column: str = Field(..., description="The column name to apply the filter on")
    operation: OperationEnum = Field(..., description="The operation to apply for filtering")
    value: FilterValue = Field(
        ...,
        description="The value to compare against. Use a list when the operation is in."
    )

    @field_validator("value")
    @classmethod
    def validate_value_shape(cls, value: FilterValue, info):
        operation = info.data.get("operation")
        is_list_value = isinstance(value, list)

        if operation == OperationEnum.in_ and not is_list_value:
            raise ValueError("The in operation requires value to be a list.")

        if operation != OperationEnum.in_ and is_list_value:
            raise ValueError("Only the in operation accepts a list value.")

        return value


class LogicalOperatorEnum(str, Enum):
    AND = "AND"
    OR = "OR"


class QueryRequest(BaseModel):
    target_table: TargetTableEnum = Field(
        ...,
        description="Choose the performance table that anchors the query."
    )

    filters: List[FilterCondition] = Field(
        default_factory=list,
        description="Filters across the selected performance table plus Athlete and Match."
    )
    logical_operator: LogicalOperatorEnum = Field(
        default=LogicalOperatorEnum.AND,
        description="The logical operator to combine multiple filter conditions (AND/OR), default is AND"
    )

    @model_validator(mode="after")
    def validate_filter_tables(self):
        allowed_tables_by_target = {
            TargetTableEnum.match_performance: {
                FilterTableEnum.match_performance,
                FilterTableEnum.athlete,
                FilterTableEnum.match,
            },
            TargetTableEnum.set_performance: {
                FilterTableEnum.set_performance,
                FilterTableEnum.athlete,
                FilterTableEnum.match,
            },
        }

        allowed_tables = allowed_tables_by_target[self.target_table]
        invalid_tables = sorted(
            {filter_condition.table.value for filter_condition in self.filters if filter_condition.table not in allowed_tables}
        )

        if invalid_tables:
            allowed_table_names = ", ".join(table.value for table in sorted(allowed_tables, key=lambda table: table.value))
            invalid_table_names = ", ".join(invalid_tables)
            raise ValueError(
                f"{self.target_table.value} queries can only use filters from {allowed_table_names}. "
                f"Invalid filter tables: {invalid_table_names}."
            )

        return self

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "target_table": "SetPerformance",
                    "filters": [
                        {
                            "table": "SetPerformance",
                            "column": "set_number",
                            "operation": "eq",
                            "value": 1,
                        },
                        {
                            "table": "Athlete",
                            "column": "country",
                            "operation": "eq",
                            "value": "TPE",
                        },
                        {
                            "table": "Match",
                            "column": "weight_class",
                            "operation": "eq",
                            "value": "-68kg",
                        },
                    ],
                    "logical_operator": "AND",
                },
                {
                    "target_table": "MatchPerformance",
                    "filters": [
                        {
                            "table": "MatchPerformance",
                            "column": "SPP",
                            "operation": "gte",
                            "value": 10,
                        },
                        {
                            "table": "Athlete",
                            "column": "name",
                            "operation": "contains",
                            "value": "Lee",
                        },
                    ],
                    "logical_operator": "AND",
                },
            ]
        }
    }