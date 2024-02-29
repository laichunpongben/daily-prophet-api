from typing import List
import logging

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from dailyprophet.feeds.portfolio import FeedPortfolio
from dailyprophet.feeds.feed_queue import FeedQueue


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

portfolio = FeedPortfolio()
queue = FeedQueue()


@app.get("/")
def landing():
    return {
        "message": "Welcome to the DailyProphet API! Check the documentation for available endpoints."
    }


@app.get("/new/{count}")
async def new(count: int):
    feeds = await portfolio.async_sample(count)
    queue.push(feeds)

    return {"message": f"{count} feeds sampled and pushed to the queue"}


@app.get("/pop")
async def pop():
    feed = queue.pop()
    size = queue.size()
    refill_threshold = 50
    refill_size = 50

    if size < refill_threshold:
        feeds = await portfolio.async_sample(refill_size)
        queue.push(feeds)
        feed = queue.pop()

    if feed:
        response = {
            "message": "The next feed is shown",
        }
        response.update(feed)
    else:
        response = {
            "message": "Error. Feed queue is empty. Please fetch new feeds, and wait for a few seconds!",
            "type": "error",
        }
    return JSONResponse(content=response)


@app.get("/pop/{count}")
def pop_many(count: int = 1):
    popped_feeds = []

    for _ in range(count):
        feed = queue.pop()
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
def reset():
    queue.clear()

    return {"message": "Queue cleared"}


@app.get("/portfolio")
def show_portfolio():
    try:
        setting = portfolio.get_setting()
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
def load_portfolio(body: PortfolioSetting):
    try:
        setting = body.setting
        portfolio.load_setting(setting)
        portfolio.save_setting_to_file()
        return {"message": "Portfolio loaded successfully"}
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error loading portfolio: {str(e)}"
        )


@app.get("/portfolio/reset")
def reset_portfolio():
    try:
        portfolio.load_setting_from_backup_file()
        portfolio.save_setting_to_file()
        setting = portfolio.get_setting()
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
