import os
from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket


@task()
def fetch(dataset_url: str):
    """getting the data from web into pandas df"""
    print(dataset_url)
    df = pd.read_csv(dataset_url, compression='gzip')
    return df


@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Fix data types"""
    df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'])
    df['dropOff_datetime'] = pd.to_datetime(df['dropOff_datetime'])
    df['PUlocationID'] = df['PUlocationID'].astype('Int64')
    df['DOlocationID'] = df['DOlocationID'].astype('Int64')
    df['SR_Flag'] = df['SR_Flag'].astype('Int64')

    print(df.head(2))
    print(f"columns: df{df.dtypes}")
    print(f"rows: {len(df)}")
    return df


@task()
def write_local(df: pd.DataFrame, dataset_file) -> Path:
    """Write dataframe locally as csv"""
    Path(f"data/fhv").mkdir(parents=True, exist_ok=True)
    path = Path(f"data/fhv/{dataset_file}.csv.gz")
    df.to_parquet(path, compression='gzip')
    return path


@task()
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return


@flow()
def etl_web_to_gcs(year, month):
    dataset_file = f"fhv_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/{dataset_file}.csv.gz"
    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean, dataset_file)
    write_gcs(path)


if __name__ == "__main__":
    year = '2019'
    for month in range(1, 13):
        etl_web_to_gcs(year, month)
