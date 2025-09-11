import csv
import googleapiclient.discovery

class YouTubeAPI:
    def __init__(self, api_key: str, channel_id: str):
        self.api_key = api_key
        self.channel_id = channel_id
        self.youtube = googleapiclient.discovery.build("youtube", "v3", developerKey=api_key)

    def get_latest_videos(self, max_results=1):
        """Ambil video terbaru dari channel (default 1 video terbaru)"""
        request = self.youtube.search().list(
            part="id",
            channelId=self.channel_id,
            maxResults=max_results,
            order="date",
            type="video"
        )
        response = request.execute()
        video_ids = [item["id"]["videoId"] for item in response["items"]]
        return video_ids

    def get_comments(self, video_id):
        """Ambil semua komentar dari sebuah video + judul video"""
        comments = []
        next_page_token = None

        # Ambil title video sekali
        video_request = self.youtube.videos().list(
            part="snippet",
            id=video_id
        )
        video_response = video_request.execute()
        video_title = video_response["items"][0]["snippet"]["title"]

        while True:
            request = self.youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=100,
                pageToken=next_page_token,
                textFormat="plainText"
            )
            response = request.execute()

            for item in response["items"]:
                top_comment = item["snippet"]["topLevelComment"]["snippet"]
                author = top_comment["authorDisplayName"]
                text = top_comment["textDisplay"]
                published_at = top_comment["publishedAt"]
                comments.append([video_id, video_title, author, text, published_at])

                # Ambil reply jika ada
                if "replies" in item:
                    for reply in item["replies"]["comments"]:
                        reply_snippet = reply["snippet"]
                        reply_author = reply_snippet["authorDisplayName"]
                        reply_text = reply_snippet["textDisplay"]
                        reply_published_at = reply_snippet["publishedAt"]
                        comments.append([video_id, video_title, reply_author, reply_text, reply_published_at])

            next_page_token = response.get("nextPageToken")
            if not next_page_token:
                break

        return comments

    def save_to_csv(self, comments, filename="comments.csv"):
        """Simpan komentar ke file CSV"""
        with open(filename, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["video_id", "video_title","author", "comment", "published_at"])
            writer.writerows(comments)
        print(f"Selesai! Komentar disimpan di {filename}")
