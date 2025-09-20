#class function
from extract.extract_youtube_comments import YouTubeAPI
from transform.preprocessing_data import TransformDataset
from transform.transform_pipeline import YouTubeDataPipeline
from load.load_to_bigquery import BigQueryHelper


#library
from dotenv import load_dotenv
import os

# === Masukkan API KEY dan Channel ID di sini ===
load_dotenv()   # path di dalam container

#SET GOOGLE_APPLICATION_CREDENTIALS BIGQUERY
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
project_id = os.getenv("PROJECT_ID")
dataset_id = os.getenv("DATASET_ID")
#DATA STAGING
table_id_staging = os.getenv("TABLE_ID_STAGING")
csv_file_staging = os.getenv("CSV_FILE_STAGING")
#DATA INTERMEDIATE
table_id_intermediate = os.getenv("TABLE_ID_INTERMEDIATE")
csv_file_intermediate = os.getenv("CSV_FILE_INTERMEDIATE")
#DATA MART
table_id_mart = os.getenv("TABLE_ID_MART")
csv_file_mart = os.getenv("CSV_FILE_MART")
#DATA WORD FREQ
table_id_word_freq = os.getenv("TABLE_ID_WORD_FREQ")
csv_file_word_freq = os.getenv("CSV_FILE_WORD_FREQ")

def main():
    yt = YouTubeAPI(os.getenv("YOUTUBE_API_KEY"), os.getenv("CHANNEL_ID"))

    print("Ambil video terbaru dari channel...")
    latest_videos = yt.get_latest_videos(max_results=10)  # ubah angka untuk ambil lebih banyak video
    print(f"Video terbaru: {latest_videos}")

    all_comments = []
    #-------------------------- this code for get all data comments ------------------------------
    # for video_id in latest_videos:
    #     print(f"Ambil komentar dari video {video_id}...")
    #     comments = yt.get_comments(video_id)
    #     print(f"Jumlah komentar untuk {video_id}: {len(comments)}")
    #     for c in comments:
    #         print(c)
    #     all_comments.extend(comments)

    #-------------------------- this code for get only 100 > data comments ------------------------------
    for video_id in latest_videos:
        print(f"Fetching comments from video {video_id}...")
        comments = yt.get_comments(video_id)
        print(f"Number of comments for {video_id}: {len(comments)}")

        if len(comments) >= 100:
            # ✅ Found a video with at least 100 comments
            selected_video = video_id
            all_comments = comments
            break
        else:
            print(f"⚠️ Skipping {video_id}, not enough comments (<100).")

    if selected_video:
        print(f"✅ Selected video: {selected_video} with {len(all_comments)} comments")
    else:
        print("❌ No video found with at least 100 comments.")
    #-------------------------- End Code for get only 100 > data comments ------------------------------

    # Simpan ke CSV
    yt.save_to_csv(all_comments, "raw_data_latest_video_comments.csv")

    # preprocessing CSV
    transform = TransformDataset("raw_data_latest_video_comments.csv")
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

    # Load data to BigQuery
    bq = BigQueryHelper(project_id=project_id, location="asia-southeast2")

    #Staging
    bq.load_csv_to_bigquery(dataset_id, table_id_staging, csv_file_staging)
    #Intermediate
    bq.load_csv_to_bigquery(dataset_id, table_id_intermediate, csv_file_intermediate)
    #Mart
    bq.load_csv_to_bigquery(dataset_id, table_id_mart, csv_file_mart)
    #Word freq
    bq.load_csv_to_bigquery(dataset_id, table_id_word_freq, csv_file_word_freq)





if __name__ == "__main__":
    main()
