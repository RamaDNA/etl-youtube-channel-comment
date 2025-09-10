import pandas as pd
import re
from textblob import TextBlob
from googletrans import Translator

class TransformDataset:
    def __init__(self, load_data_csv):
        self.load_data_csv = pd.read_csv(load_data_csv)
    
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
    
    def translate_to_english(self, text):
        try:
            translated = self.translator.translate(str(text), src="id", dest="en")
            return translated.text
        except Exception as e:
            return str(text)  # kalau error, balikin text aslinya

    def get_sentiment(self, text):
        # Use TextBlob for sentiment polarity
        blob = TextBlob(str(text))
        return blob.sentiment.polarity

    def preprocessing_data_comments(self):
        """
        Preprocess comment text for sentiment analysis and add new features.
        Returns: DataFrame with cleaned text, sentiment, and new features.
        """
        df = self.load_data_csv.copy()
        # Clean comment text
        df["clean_comment"] = df["comment"].apply(self.clean_text)
        # translate to english for textblob
        df["translate_english"] = df["clean_comment"].apply(self.translate_to_english)
        # Sentiment polarity
        df["sentiment"] = df["translate_english"].apply(self.get_sentiment)
        # Comment length
        df["comment_length"] = df["clean_comment"].apply(len)
        # Word count
        df["word_count"] = df["clean_comment"].apply(lambda x: len(x.split()))
        # Has emoji
        df["has_emoji"] = df["comment"].apply(lambda x: bool(re.search(r'[\U0001F600-\U0001F64F]', str(x))))

        field_user = ["clean_comment","word_count","sentiment","published_at"]
        new_df = df[field_user]
        return new_df

    def save_to_csv(self, output_path):
        """
        Run preprocessing and save result to a new CSV file.
        """
        df = self.preprocessing_data_comments()
        df.to_csv(output_path, index=False)
        print(f"Dataset berhasil disimpan ke {output_path}")
        return df
