from typing import Any, Dict, List, Type

from sqlalchemy import String, and_, cast, or_
from sqlalchemy.orm import Session

from app.db.models import Match, Athlete, MatchPerformance, SetPerformance
from app.schemas.query_schema import (
    TargetTableEnum,
    FilterTableEnum,
    OperationEnum,
    FilterCondition,
    LogicalOperatorEnum,
    QueryRequest
    )


TARGET_MODEL_MAP: Dict[TargetTableEnum, Type[MatchPerformance] | Type[SetPerformance]] = {
    TargetTableEnum.match_performance: MatchPerformance,
    TargetTableEnum.set_performance: SetPerformance,
}

FILTER_MODEL_MAP: Dict[FilterTableEnum, Type[Athlete] | Type[Match] | Type[MatchPerformance] | Type[SetPerformance]] = {
    FilterTableEnum.athlete: Athlete,
    FilterTableEnum.match: Match,
    FilterTableEnum.match_performance: MatchPerformance,
    FilterTableEnum.set_performance: SetPerformance,
}


def _get_filter_column(filter_condition: FilterCondition):
    model = FILTER_MODEL_MAP[filter_condition.table]

    if filter_condition.column not in model.__table__.columns.keys():
        raise ValueError(
            f"{filter_condition.table.value} does not have column '{filter_condition.column}'."
        )

    return getattr(model, filter_condition.column)


def _build_filter_expression(filter_condition: FilterCondition):
    column = _get_filter_column(filter_condition)
    value = filter_condition.value

    if filter_condition.operation == OperationEnum.eq:
        return column == value

    if filter_condition.operation == OperationEnum.neq:
        return column != value

    if filter_condition.operation == OperationEnum.gt:
        return column > value

    if filter_condition.operation == OperationEnum.gte:
        return column >= value

    if filter_condition.operation == OperationEnum.lt:
        return column < value

    if filter_condition.operation == OperationEnum.lte:
        return column <= value

    if filter_condition.operation == OperationEnum.contains:
        if not isinstance(value, str):
            raise ValueError("The contains operation requires a string value.")
        return cast(column, String).ilike(f"%{value}%")

    if filter_condition.operation == OperationEnum.in_:
        return column.in_(value)

    raise ValueError(f"Unsupported filter operation: {filter_condition.operation.value}")


def _serialize_model(instance: Any) -> Dict[str, Any]:
    return {
        column.name: getattr(instance, column.name)
        for column in instance.__table__.columns
    }


def get_filtered_matches(db: Session, request: QueryRequest) -> List[Dict[str, Any]]:
    performance_model = TARGET_MODEL_MAP[request.target_table]

    query = (
        db.query(performance_model, Athlete, Match)
        .join(Athlete, performance_model.athlete_id == Athlete.athlete_id)
        .join(Match, performance_model.match_id == Match.match_id)
    )

    if request.filters:
        expressions = [_build_filter_expression(filter_condition) for filter_condition in request.filters]

        if request.logical_operator == LogicalOperatorEnum.AND:
            query = query.filter(and_(*expressions))
        else:
            query = query.filter(or_(*expressions))

    results = query.all()

    return [
        {
            "target_table": request.target_table.value,
            "performance": _serialize_model(performance),
            "athlete": _serialize_model(athlete),
            "match": _serialize_model(match_record),
        }
        for performance, athlete, match_record in results
    ]
