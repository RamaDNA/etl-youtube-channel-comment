import pandas as pd
import re
from textblob import TextBlob
from googletrans import Translator
from sastrawi.Steamer.StemmerFactory import StemmerFactory

class TransformDataset:
    def __init__(self, load_data_csv):
        self.load_data_csv = pd.read_csv(load_data_csv)
        self.translator = Translator() # for translator
        # Stemmer Sastrawi
        factory = StemmerFactory()
        self.stemmer = factory.create_stemmer()
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
    
    def tokenize(self, text):
        return text.split()
    
    def stemming(self, text):
        return self.stemmer.stem(text)

    def translate_to_english(self, text): #translate not working fix it
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
        df = self.load_data_csv.copy()
        # Cleaning
        df["clean_comment"] = df["comment"].apply(self.clean_text)
        # Drop kosong setelah cleaning
        df = df[df["clean_comment"].str.strip() != ""].copy()
        df.reset_index(drop=True, inplace=True)
        # Normalisasi slang
        df["normalized"] = df["clean_comment"].apply(self.normalize_slang)
        # Word elongation handling
        df["shortened"] = df["normalized"].apply(self.handle_word_elongation)
        # Stemming
        df["stemmed"] = df["shortened"].apply(self.stemming)
        # Tokenization
        df["tokens"] = df["stemmed"].apply(self.tokenize)
        # Translate
        df["translate_english"] = df["stemmed"].apply(self.translate_to_english)
        # Sentiment
        df["sentiment"] = df["translate_english"].apply(self.get_sentiment)
        
        field_user = ["clean_comment", "normalized", "shortened", "stemmed", "tokens", 
                      "translate_english", "sentiment", "published_at"]
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
