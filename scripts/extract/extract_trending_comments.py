import csv
from googleapiclient.discovery import build


class YouTubeTrendingAPI:
    def __init__(self, api_key: str):
        """Inisialisasi client YouTube API"""
        self.youtube = build("youtube", "v3", developerKey=api_key)

    def get_trending_videos(self, max_results=5, region_code="ID"):
        """Ambil video trending dari region tertentu (default Indonesia)"""
        request = self.youtube.videos().list(
            part="id,snippet",
            chart="mostPopular",
            regionCode=region_code,
            maxResults=min(max_results, 50)  # API max 50
        )
        response = request.execute()
        video_items = response.get("items", [])
        videos = [
            {
                "video_id": item["id"],
                "title": item["snippet"]["title"]
            }
            for item in video_items
        ]
        return videos

    def get_comments(self, video_id: str, video_title: str, limit=200):
        """Ambil komentar dari video tertentu"""
        comments = []

        request = self.youtube.commentThreads().list(
            part="snippet,replies",
            videoId=video_id,
            maxResults=min(limit, 100),
            textFormat="plainText"
        )
        response = request.execute()

        for item in response.get("items", []):
            top_comment = item["snippet"]["topLevelComment"]["snippet"]
            text = top_comment["textDisplay"]
            published_at = top_comment["publishedAt"]
            comments.append([video_id, video_title, text, published_at])

            # Ambil replies (opsional)
            if "replies" in item:
                for reply in item["replies"]["comments"]:
                    reply_snippet = reply["snippet"]
                    reply_text = reply_snippet["textDisplay"]
                    reply_published_at = reply_snippet["publishedAt"]
                    comments.append([video_id, video_title, reply_text, reply_published_at])

        return comments

    def save_to_csv(self, comments, filename="comments.csv"):
        """Simpan komentar ke file CSV"""
        with open(filename, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["video_id", "video_title", "comment", "published_at"])
            writer.writerows(comments)
        print(f"Selesai! Komentar disimpan di {filename}")
