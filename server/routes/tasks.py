import os

from fastapi import APIRouter, HTTPException
from starlette import status

from db.task_db import get_task_repo

router = APIRouter()

@router.get("/tasks/{task_id}", name="get_task_status", status_code=status.HTTP_200_OK)
async def get_task_status(task_id: str):
    repo = get_task_repo()
    task = repo.get_task(task_id)

    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    if task.status == "completed":
        directory = os.path.dirname(task.file_name)
        output_path = os.path.join(directory, "ai_response.txt")

        if not os.path.exists(output_path):
            raise HTTPException(status_code=500, detail="AI response file missing")

        with open(output_path, "r", encoding="utf-8") as f:
            content = f.read()

        return {
            "taskId": task_id,
            "status": "completed",
            "content": content
        }

    return {"taskId": task_id, "status": task.status}