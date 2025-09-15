#class function
from youtube_api import YouTubeAPI
from dataset_comment_transform import TransformDataset
from data_transform_pipeline import YouTubeDataPipeline


#library
from dotenv import load_dotenv
import os

# === Masukkan API KEY dan Channel ID di sini ===
load_dotenv()   # path di dalam container

def main():
    yt = YouTubeAPI(os.getenv("YOUTUBE_API_KEY"), os.getenv("CHANNEL_ID"))

    print("Ambil video terbaru dari channel...")
    latest_videos = yt.get_latest_videos(max_results=5)  # ubah angka untuk ambil lebih banyak video
    print(f"Video terbaru: {latest_videos}")

    all_comments = []
    for video_id in latest_videos:
        print(f"Ambil komentar dari video {video_id}...")
        comments = yt.get_comments(video_id)
        print(f"Jumlah komentar untuk {video_id}: {len(comments)}")
        for c in comments:
            print(c)
        all_comments.extend(comments)

    # Simpan ke CSV
    yt.save_to_csv(all_comments, "latest_video_comments.csv")

    # preprocessing CSV
    transform = TransformDataset("latest_video_comments.csv")
    transform.save_to_csv("processed/staging.csv")

    pipeline = YouTubeDataPipeline("processed/staging.csv", output_dir="processed")

    # Staging
    pipeline.create_staging()
    pipeline.save_staging()

    # Intermediate
    pipeline.create_intermediate()
    pipeline.save_intermediate()

    # Data Mart
    pipeline.create_data_mart()
    pipeline.save_data_mart()





if __name__ == "__main__":
    main()
