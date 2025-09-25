#class function
from extract.extract_youtube_comments import YouTubeAPI
from extract.extract_trending_comments import YouTubeTrendingAPI
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

    #-------------------------- this code for get only 100 > data comments ------------------------------

    # yt = YouTubeAPI(os.getenv("YOUTUBE_API_KEY"), os.getenv("CHANNEL_ID"))
    # print("Ambil video terbaru dari channel...")

    # max_results = 10
    # selected_videos = []     # save video_id 
    # all_comments = {}        # save comments {video_id: comments}
    # checked_videos = set()   # save video_id has been checked
    # need_video = 1           # jumlah video yang dibutuhkan

    # while len(selected_videos) < need_video:  
    #     latest_videos = yt.get_latest_videos(max_results=max_results)
    #     print(f"🔄 Ambil {max_results} video terbaru...")

    #     for video_id in latest_videos:
    #         if video_id in checked_videos:
    #             continue  # skip kalau sudah pernah dicek

    #         print(f"Fetching comments from video {video_id}...")
    #         comments = yt.get_comments(video_id)
    #         print(f"Number of comments for {video_id}: {len(comments)}")

    #         checked_videos.add(video_id)  # tandai sudah dicek

    #         if len(comments) >= 5:
    #             selected_videos.append(video_id)
    #             all_comments[video_id] = comments
    #             print(f"✅ Selected video: {video_id} with {len(comments)} comments "
    #                 f"(total: {len(selected_videos)}/{need_video})")

    #             if len(selected_videos) == need_video:
    #                 break
    #         else:
    #             print(f"⚠️ Skipping {video_id}, not enough comments (<100).")

    #     if len(selected_videos) < need_video:
    #         print(f"❌ Belum cukup video ≥100 komentar. Tambah max_results jadi {max_results + 1}")
    #         max_results += 1

    # print("\n🎉 Selesai, daftar video yang lolos:")
    # for vid in selected_videos:
    #     print(f"- {vid} dengan {len(all_comments[vid])} komentar")

    # # ==================================================
    # # 🔹 Flatten dictionary ke list supaya tidak kosong
    # # ==================================================
    # flat_comments = []
    # for vid, comments in all_comments.items():
    #     flat_comments.extend(comments)   # gabung semua komentar

    # print(f"\nTotal komentar yang dikumpulkan: {len(flat_comments)}")

    #-------------------------- End Code for get only 100 > data comments ------------------------------
    
    #-------------------------- Start Code for get trending data > data comments ------------------------------
    yt = YouTubeTrendingAPI(os.getenv("YOUTUBE_API_KEY"))
    print("Ambil video trending...")

    print("Mengambil video trending...")
    videos = yt.get_trending_videos(max_results=3, region_code="ID")

    all_comments = []
    for v in videos:
        video_id = v["video_id"]
        video_title = v["title"]
        print(f"Fetching comments from video {video_id} - {video_title} ...")
        comments = yt.get_comments(video_id, video_title, limit=200)
        all_comments.extend(comments)

    #-------------------------- End Code for get trending data > data comments ------------------------------
    # Simpan ke CSV
    yt.save_to_csv(all_comments, "raw_data_latest_video_comments.csv")

    # preprocessing CSV
    transform = TransformDataset("raw_data_latest_video_comments.csv")
    transform.save_to_csv("/opt/airflow/scripts/processed/staging.csv")

    pipeline = YouTubeDataPipeline("/opt/airflow/scripts/processed/staging.csv", output_dir="processed")

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
