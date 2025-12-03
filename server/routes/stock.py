import logging
from fastapi import APIRouter, HTTPException, status, Request, Response
from uuid import uuid4

from db.task_db import get_task_repo
from models.task import Task

logger = logging.getLogger(__name__)
router = APIRouter()
@router.post("/stock", status_code=status.HTTP_202_ACCEPTED)
async def create_stock_task(
    symbol: str,
    request: Request,
    response: Response
):
    if not symbol:
        raise HTTPException(status_code=400, detail="symbol is required")

    task_id = str(uuid4())
    logger.info(f"Creating task {task_id} for symbol {symbol}")

    new_task = Task.new(symbol=symbol.upper(), task_id=task_id)
    repo = get_task_repo()
    repo.insert_task(new_task)

    # Build Location header pointing to the task resource
    location_url = request.url_for("get_task_status", task_id=task_id)
    response.headers["Location"] = str(location_url)

    return {
        "taskId": task_id,
        "status": "submitted",
    }

