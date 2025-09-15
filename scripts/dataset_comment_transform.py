import pandas as pd
import re
from deep_translator import GoogleTranslator
from Sastrawi.Stemmer.StemmerFactory import StemmerFactory
from Sastrawi.StopWordRemover.StopWordRemoverFactory import StopWordRemoverFactory

#for sentiment prediction my device can't handle it
# from transformers import pipeline 

class TransformDataset:
    def __init__(self, load_data_csv):
        self.load_data_csv = pd.read_csv(load_data_csv)
        # Stemmer Sastrawi
        factory = StemmerFactory()
        self.stemmer = factory.create_stemmer()

        #stopword remover Sastrawi
        stop_factory = StopWordRemoverFactory()
        self.stopwords = stop_factory.create_stop_word_remover()

        # dictionary slang indonesian
        self.slang_dict = {
            "gk": "tidak",
            "ga": "tidak",
            "tdk": "tidak",
            "yg": "yang",
            "dlm": "dalam",
            "sm": "sama",
            "dr": "dari",
            "tp": "tapi",
            "trs": "terus",
            "aja": "saja",
            "udh": "sudah",
            "udh": "sudah",
            "klo": "kalau",
            "blm": "belum",
            "btw": "by the way",
        }

        # Sentiment classifier (zero-shot) my device can't handle it
        # self.classifier = pipeline(
        #     "zero-shot-classification",
        #     model="joeddav/xlm-roberta-large-xnli"
        # )
        # self.labels = ["positif", "negatif", "netral"]
    
    def clean_text(self, text):
        # Lowercase
        text = str(text).lower()
        # Remove URLs
        text = re.sub(r"http\S+|www.\S+", "", text)
        # Remove non-alphabetic characters (keep emoticons)
        text = re.sub(r"[^a-zA-Z\s]", "", text)
        # Remove extra spaces
        text = re.sub(r"\s+", " ", text).strip()
        return text
    
    def normalize_slang(self, text):
        tokens = text.split()
        tokens = [self.slang_dict.get(word, word) for word in tokens]
        return " ".join(tokens)
    
    def handle_word_elongation(self, text):
        # "baaaagusss" -> "bagus"
        return re.sub(r'(.)\1{2,}', r'\1', text)
    
    def remove_stopwords(self, text):
        return self.stopwords.remove(text)

    def tokenize(self, text):
        return text.split()
    
    def stemming(self, text):
        return self.stemmer.stem(text)

    def translate_to_english(self, text):
        try:
            translated = GoogleTranslator(source="id", target="en").translate(str(text))
            return translated
        except Exception:
            return str(text)

    def get_sentiment(self, text):
        """
        Zero-shot sentiment classification: positif, negatif, netral
        """
        if not text or str(text).strip() == "":
            return "netral"
        result = self.classifier(str(text), candidate_labels=self.labels)
        return result["labels"][0]

    def preprocessing_data_comments(self):
        df = self.load_data_csv.copy()

        # make sure have some video
        required_cols = {"video_title", "comment", "published_at"}
        missing = required_cols - set(df.columns)
        if missing:
            raise ValueError(f"Kolom berikut tidak ada di CSV: {missing}")

        # Cleaning text
        df["clean_comment"] = df["comment"].apply(self.clean_text)

        # drop null data because sometimes only using emote for comment
        df = df[df["clean_comment"].str.strip() != ""].copy()
        df.reset_index(drop=True, inplace=True)

        # normalization slang words
        df["normalized"] = df["clean_comment"].apply(self.normalize_slang)

        # Word elongation handling
        df["shortened"] = df["normalized"].apply(self.handle_word_elongation)

        # Stemming
        df["stemmed"] = df["shortened"].apply(self.stemming)

        #stopword removal
        df["stopword"] = df["stemmed"].apply(self.remove_stopwords)

        # Tokenization
        df["tokens"] = df["stopword"].apply(self.tokenize)

        # # Translate couse not accurate for sentiment
        # df["translate_english"] = df["shortened"].apply(self.translate_to_english)

        # Sentiment using (zero-shot) my device can't handle it
        # df["sentiment"] = df["shortened"].apply(self.get_sentiment)

        # change datetime UTC
        df["published_at"] = pd.to_datetime(df["published_at"], utc=True)

        # convert to WIB (Asia/Jakarta)
        df["published_at_wib"] = df["published_at"].dt.tz_convert("Asia/Jakarta")

        # take year published
        df["year_wib"] = df["published_at_wib"].dt.year

        # take mounth published (0-12)
        df["month_wib"] = df["published_at_wib"].dt.month

        # take day published
        df["day_wib"] = df["published_at_wib"].dt.day

        # take hour published (0-23)
        df["hour_wib"] = df["published_at_wib"].dt.hour

        field_user = [
            "video_id",
            "video_title",
            "stopword",
            "tokens",
            # "sentiment", this si for sentiment prediction my device can't handle it
            "published_at_wib",
            "year_wib",
            "month_wib",
            "day_wib",
            "hour_wib"
        ]
        new_df = df[field_user]
        # drop duplicates jika ada
        new_df = new_df.drop_duplicates(subset="stopword")

        return new_df


    def save_to_csv(self, output_path):
        """
        Run preprocessing and save result to a new CSV file.
        """
        df = self.preprocessing_data_comments()
        df.to_csv(output_path, index=False)
        print(f"Dataset berhasil disimpan ke {output_path}")
        return df
