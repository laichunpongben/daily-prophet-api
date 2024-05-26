## DailyProphet: Your Personalized Daily News Aggregator

**Tired of scrolling through endless, irrelevant feeds?** DailyProphet solves the problem by providing a **personalized news aggregator** that puts you in control of your information consumption. This repository contains the backend part of the project.

**Key Features:**

* **Curated feeds:** Choose the topics and sources you want to see, and DailyProphet will deliver relevant news directly to you. 
* **Concise summaries:** Get the most important information from your chosen sources, without having to click through dozens of irrelevant articles.
* **Easy reading:**  Organize your preferred sources and topics in one place for effortless access to the information that matters most.

**The Problem:**

* **Irrelevant information:** Most feeds rely on algorithms that guess your preferences based on past browsing history, often leading to inaccurate or irrelevant content. 
* **Scattered sources:** Finding the information you need can be a time-consuming task, as interesting subjects are dispersed across various websites.
* **Information overload:** Even with relevant information, it can be overwhelming to keep up with the sheer volume of content available. 

**DailyProphet's Solution:**

DailyProphet addresses these issues by:

* **Giving you control:** Customize your feeds with your preferred sources and topics.
* **Consolidating your information:** Aggregate your chosen sources into a single, unified feed.
* **Providing summaries:** Get to the point quickly with concise, easy-to-read summaries of the most relevant information.

**Getting Started:**

1. **Login:** Create a personal profile to personalize your experience.
2. **Setting:** Add topics that interest you and choose your preferred sources.
3. **Feed:** Relax and enjoy reading!

**Code Structure:**

This project is built with a Python backend using FastAPI. Here's a breakdown of the code structure:

* **`main.py`:** The FastAPI application entry point, handling API routes and requests.
* **`auth.py`:** Authentication and authorization logic using JWT tokens.
* **`readers`:**
    * **`reader_manager.py`:**  Manages user-specific reader instances.
    * **`reader.py`:** Represents a single user's reader with its own feed queue and portfolio.
* **`feeds`:**
    * **`feed_factory.py`:** Creates instances of specific feed types.
    * **`feed.py`:**  Base class for all feed types.
    * **`arxiv.py`:**  ArXiv feed implementation.
    * **`reddit.py`:**  Reddit feed implementation.
    * **`youtube.py`:**  YouTube feed implementation.
    * **`openweathermap.py`:**  OpenWeatherMap feed implementation.
    * **`lihkg.py`:**  LIHKG feed implementation.
    * **`portfolio.py`:**  Manages user-defined feed preferences and weights.
    * **`feed_queue.py`:**  Manages the feed queue for each reader. 
* **`util.py`:** Utility functions for common tasks like weighted sampling and background tasks.
* **`mongodb_service.py`:**  Handles interactions with the MongoDB database.
* **`configs.py`:** Stores configuration settings and API keys.

**Contributing:**

Contributions are welcome! Feel free to submit issues, feature requests, or pull requests.

**License:**

This project is licensed under the MIT License.

**Getting Started with the Backend:**

1. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```
2. **Set up MongoDB:**
   * Create a MongoDB Atlas cluster.
   * Configure the connection string in `configs.py`.
3. **Set up API Keys:**
   * Obtain API keys from the following services and add them to `configs.py`:
     * OpenWeatherMap
     * YouTube
     * Reddit
     * LIHKG
     * Foursquare
4. **Start the server:**
   ```bash
   uvicorn main:app --host 0.0.0.0 --port 8000 --log-config log_conf.yaml
   ```
5. **Access the API:**
   The DailyProphet API is now running on `http://localhost:8000`. Refer to the code for available endpoints.

**Next Steps:**

* **Frontend Development:** Build a user-friendly frontend interface to interact with the API.
* **More Sources:** Add support for more news sources.
* **Advanced Features:** Explore features like sentiment analysis, topic extraction, and personalized recommendations.

**Join the journey to reclaim control over your daily information consumption with DailyProphet!**
