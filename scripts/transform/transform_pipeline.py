import pandas as pd
import ast
from pathlib import Path
from collections import Counter

class YouTubeDataPipeline:
    def __init__(self, csv_file: str, output_dir: str = "outputs"):
        self.raw = pd.read_csv(csv_file)
        self.staging = None
        self.intermediate = None
        self.data_mart = None
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    # 1. STAGING LAYER
    def create_staging(self):
        df = self.raw.copy()
        df = df.dropna(subset=["stopword", "tokens"])

        # make sure ur data is list not the string
        df["tokens"] = df["tokens"].apply(
            lambda x: ast.literal_eval(x) if isinstance(x, str) else x
        )
        df["stopword"] = df["stopword"].str.lower()

        self.staging = df
        return df

    def save_staging(self, filename="staging.csv"):
        if self.staging is None:
            raise ValueError("Staging belum dibuat. Jalankan create_staging dulu.")
        path = self.output_dir / filename
        self.staging.to_csv(path, index=False)
        print(f"✅ Staging disimpan ke {path}")

    # 2. INTERMEDIATE LAYER
    def create_intermediate(self):
        if self.staging is None:
            raise ValueError("Staging belum dibuat! Jalankan create_staging dulu.")
        
        df = self.staging.copy()
        df["comment_length"] = df["stopword"].apply(len)
        df["word_count"] = df["tokens"].apply(len)
        df["published_at_wib"] = pd.to_datetime(df["published_at_wib"])
        df["year_wib"] = df["published_at_wib"].dt.year
        df["month_wib"] = df["published_at_wib"].dt.month
        df["day_wib"] = df["published_at_wib"].dt.day
        df["weekday_wib"] = df["published_at_wib"].dt.day_name()

        df["hour_bin"] = pd.cut(
            df["hour_wib"],
            bins=[0, 6, 12, 18, 24], 
            labels=["Malam", "Pagi", "Siang", "Malam2"], 
            right=False
        )

            # 🔑 Word frequency per video_id
        word_freq = (
            df.explode("tokens")  # explode token lists into rows
            .groupby(["video_id", "tokens"])  # count per video_id and word
            .size()
            .reset_index(name="count")
            .rename(columns={"tokens": "word"})
            .sort_values(["video_id", "count"], ascending=[True, False])
        )
        self.word_freq = word_freq

        self.intermediate = df
        return df

    def save_intermediate(self, filename="intermediate.csv", word_freq_file="word_freq.csv"):
        if self.intermediate is None:
            raise ValueError("Intermediate belum dibuat. Jalankan create_intermediate dulu.")
        
        path = self.output_dir / filename
        self.intermediate.to_csv(path, index=False)
        print(f"✅ Intermediate disimpan ke {path}")

        # simpan word frequency
        if hasattr(self, "word_freq"):
            wf_path = self.output_dir / word_freq_file
            self.word_freq.to_csv(wf_path, index=False)
            print(f"✅ Word frequency disimpan ke {wf_path}")

    # 3. DATA MART LAYER
    def create_data_mart(self):
        if self.intermediate is None:
            raise ValueError("Intermediate belum dibuat! Jalankan create_intermediate dulu.")
        
        df = self.intermediate.copy()
        mart = df.groupby(
            ["video_id", "video_title", "year_wib", "month_wib", "day_wib", "hour_bin"],
            observed=True
        ).agg(
            total_comments=("stopword", "count"),
            avg_length=("comment_length", "mean"),
            avg_word_count=("word_count", "mean")
        ).reset_index()
        self.data_mart = mart
        return mart

    def save_data_mart(self, filename="data_mart.csv"):
        if self.data_mart is None:
            raise ValueError("Data mart belum dibuat. Jalankan create_data_mart dulu.")
        path = self.output_dir / filename
        self.data_mart.to_csv(path, index=False)
        print(f"✅ Data mart disimpan ke {path}")
