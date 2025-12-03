from fastapi import FastAPI

from server.routes import stock, tasks
app = FastAPI(title="Stocker API", version="1.0.0")

app.include_router(stock.router, tags=["Stock"])
app.include_router(tasks.router, tags=["Tasks"])

@app.get("/health")
async def health():
    return {"status": "ok"}