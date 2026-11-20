"""小数列精度迁移接口：试运行、执行、续做、下游查询和目标核对。

这些接口直接操作进程内迁移状态，路径挂在 /api/decimal 下，
避免和通用资源路由 /api/{module} 的动态路径混淆（固定路径先注册）。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from app.services.decimal_migration import migration_service

router = APIRouter(prefix="/api/decimal", tags=["小数列迁移"])


class ExecutePayload(BaseModel):
    batch_id: str | None = None
    columns: list[str] | None = None
    strict_batch: bool | None = None
    target_table: str | None = None


class DryRunPayload(BaseModel):
    batch_id: str | None = None
    strict_batch: bool | None = None
    target_table: str | None = None


def _handle(call, *args: Any, **kwargs: Any) -> Any:
    try:
        return call(*args, **kwargs)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc).strip("'")) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/batches")
def list_batches() -> dict[str, Any]:
    return {"items": migration_service.list_batches()}


@router.get("/batch")
def get_batch(batch_id: str | None = Query(default=None)) -> dict[str, Any]:
    return _handle(migration_service.get_batch, batch_id)


@router.post("/dry-run")
def dry_run(payload: DryRunPayload | None = None) -> dict[str, Any]:
    payload = payload or DryRunPayload()
    return _handle(
        migration_service.dry_run,
        payload.batch_id,
        strict_batch=payload.strict_batch,
        target_table=payload.target_table,
    )


@router.post("/execute")
def execute(payload: ExecutePayload | None = None) -> dict[str, Any]:
    payload = payload or ExecutePayload()
    return _handle(
        migration_service.execute,
        payload.batch_id,
        columns=payload.columns,
        strict_batch=payload.strict_batch,
        target_table=payload.target_table,
    )


@router.post("/resume")
def resume(payload: ExecutePayload | None = None) -> dict[str, Any]:
    payload = payload or ExecutePayload()
    return _handle(
        migration_service.resume,
        payload.batch_id,
        target_table=payload.target_table,
    )


@router.get("/records")
def records(batch_id: str | None = Query(default=None)) -> dict[str, Any]:
    return {"items": migration_service.migration_records(batch_id)}


@router.get("/downstream")
def downstream(
    name: str = Query(..., description="下游名称"),
    batch_id: str | None = Query(default=None),
) -> dict[str, Any]:
    return _handle(migration_service.query_downstream, name, batch_id)


@router.get("/target")
def target(batch_id: str | None = Query(default=None)) -> dict[str, Any]:
    batch = _handle(migration_service.get_batch, batch_id)
    return migration_service.target_snapshot(batch["target_table"])


@router.get("/verify")
def verify(batch_id: str | None = Query(default=None)) -> dict[str, Any]:
    return _handle(migration_service.verify_target, batch_id)


@router.get("/column-changes")
def column_changes(table: str | None = Query(default=None)) -> dict[str, Any]:
    return migration_service.column_changes(table)


@router.get("/runs")
def runs(batch_id: str | None = Query(default=None)) -> dict[str, Any]:
    return {"items": migration_service.runs(batch_id)}


@router.post("/reset")
def reset() -> dict[str, Any]:
    migration_service.reset()
    return {"ok": True, "message": "迁移演示状态已重置"}
