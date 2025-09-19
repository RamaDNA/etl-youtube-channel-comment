import os
from google.cloud import bigquery

class BigQueryHelper:
    def __init__(self, project_id: str, location: str = "US"):
        self.client = bigquery.Client(project=project_id)
        self.location = location

    def create_dataset_if_not_exists(self, dataset_id: str):
        """Buat dataset jika belum ada"""
        dataset_ref = self.client.dataset(dataset_id)

        try:
            self.client.get_dataset(dataset_ref)
            print(f"Dataset {dataset_id} sudah ada.")
        except Exception:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = self.location
            self.client.create_dataset(dataset)
            print(f"Dataset {dataset_id} berhasil dibuat.")

    def load_csv_to_bigquery(self, dataset_id: str, table_id: str, csv_file: str):
        """Load CSV ke BigQuery"""
        # Pastikan dataset ada
        self.create_dataset_if_not_exists(dataset_id)

        table_ref = self.client.dataset(dataset_id).table(table_id)
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            autodetect=True,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )

        with open(csv_file, "rb") as source_file:
            job = self.client.load_table_from_file(source_file, table_ref, job_config=job_config)

        job.result()
        print(f"CSV {csv_file} berhasil di-load ke {dataset_id}.{table_id}")
