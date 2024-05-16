from typing import List
import asyncio
import logging

from fastapi import FastAPI, HTTPException, BackgroundTasks, Depends
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from .readers.reader_manager import ReaderManager
from .auth import get_current_user
from .util import async_wake_up_worker


class PortfolioSetting(BaseModel):
    setting: List


logger = logging.getLogger(__name__)

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins="*",  # Allow all origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

reader_manager = ReaderManager()


@app.get("/")
def landing():
    return {
        "message": "Welcome to the DailyProphet API! Check the documentation for available endpoints."
    }


@app.get("/new/{count}")
async def new(
    count: int,
    current_user: str = Depends(get_current_user),
):
    reader = reader_manager[current_user]

    await reader.async_new(count)

    return {"message": f"{count} feeds sampled and pushed to the queue"}


@app.get("/pop")
async def pop(
    background_tasks: BackgroundTasks,
    current_user: str = Depends(get_current_user),
):
    # workaround to keep the worker warm when a user is using the service
    _ = asyncio.create_task(async_wake_up_worker())

    reader = reader_manager[current_user]

    feed = await reader.async_pop()
    size = reader.queue.size()
    refill_threshold = 50
    refill_size = 50

    async def refill_queue(refill_size: int):
        await reader.async_new(refill_size)

    if size < refill_threshold:
        # Schedule the async_sample to run in the background
        background_tasks.add_task(refill_queue, refill_size)

    if feed:
        response = {
            "message": "The next feed is shown",
        }
        response.update(feed)
    else:
        response = {
            "message": "Error. We are pulling new feeds to the queue. Please wait for a few seconds!",
            "type": "error",
        }
    return JSONResponse(content=response)


@app.get("/pop/{count}")
async def pop_many(
    count: int = 1,
    current_user: str = Depends(get_current_user),
):
    reader = reader_manager[current_user]

    popped_feeds = []

    for _ in range(count):
        feed = await reader.async_pop()
        if feed:
            popped_feeds.append(feed)

    if popped_feeds:
        response = {
            "message": f"{count} feed(s) popped successfully",
            "feeds": popped_feeds,
        }
    else:
        response = {
            "message": "Error. Feed queue is empty. Please fetch new feeds, and wait for a few seconds!",
            "type": "error",
        }

    return JSONResponse(content=response)


@app.get("/reset")
def reset(
    current_user: str = Depends(get_current_user),
):
    reader = reader_manager[current_user]

    reader.queue.clear()

    return {"message": "Queue cleared"}


@app.get("/portfolio")
def show_portfolio(
    current_user: str = Depends(get_current_user),
):
    try:
        reader = reader_manager[current_user]

        setting = reader.portfolio.get_setting()
        return {
            "message": "Portfolio shown successfully",
            "type": "portfolio",
            "setting": setting,
        }
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error showing portfolio: {str(e)}"
        )


@app.post("/portfolio")
def update_portfolio(
    body: PortfolioSetting,
    current_user: str = Depends(get_current_user),
):
    try:
        reader = reader_manager[current_user]

        setting = body.setting
        reader.portfolio.load_setting(setting)

        reader_manager.sync()
        reader.queue.trim_last_until(10)
        return {"message": "Portfolio loaded successfully"}
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error loading portfolio: {str(e)}"
        )


@app.get("/portfolio/reset")
def reset_portfolio(
    current_user: str = Depends(get_current_user),
):
    try:
        reader = reader_manager[current_user]

        reader.portfolio.load_default()
        setting = reader.portfolio.get_setting()

        reader_manager.sync()
        reader.queue.trim_last_until(10)

        return {
            "message": "Portfolio reset successfully",
            "type": "portfolio",
            "setting": setting,
        }
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error resetting portfolio: {str(e)}"
        )


if __name__ == "__main__":
    import os

    import uvicorn

    log_conf_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "../log_conf.yaml"
    )
    uvicorn.run(app, host="127.0.0.1", port=8000, log_config=log_conf_path)
