# Get YouTube Comments 

This project allows you to fetch comments from YouTube using the **YouTube Data API v3** and run it with **Docker Compose v3.9**.

---

## Prerequisites

Before running the project, make sure you have:

1. **Docker & Docker Compose**  
   - Install Docker: [https://docs.docker.com/get-docker/](https://docs.docker.com/get-docker/)  
   - Install Docker Compose: [https://docs.docker.com/compose/install/](https://docs.docker.com/compose/install/)

2. **YouTube Data API Key**  
   - Tutorial to create an API Key: [YouTube API - Getting Started](https://developers.google.com/youtube/v3/getting-started)

3. **Google Cloud Service Account Key (`gcp_key.json`)**  
   - Tutorial to create a Service Account Key: [Google Cloud - Service Accounts](https://cloud.google.com/iam/docs/creating-managing-service-accounts)

---

## Setup

1. Create a file named `.env` in the project root and add the following variables:

- `YOUTUBE_API_KEY` – Your YouTube API Key  
- `CHANNEL_ID` – The ID of the YouTube channel you want to fetch comments from

2. Place the `gcp_key.json` file in the root project directory config.


---

## Running the Project

Once setup is complete, run the following command to start the Docker container:

```bash
docker-compose up
