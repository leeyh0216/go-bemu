# bqemu:operation bigquery.datasets.list.filter scenario=dataset-label-filter
# bqemu:operation bigquery.datasets.get.metadata-view scenario=dataset-metadata-view
from google.cloud import bigquery


def test_official_client_filters_datasets_and_requests_metadata_view(
    bq_client: bigquery.Client, project_id: str
) -> None:
    receiving = bigquery.Dataset(f"{project_id}.receiving")
    receiving.location = "US"
    receiving.labels = {"department": "receiving", "active": "true"}
    bq_client.create_dataset(receiving)

    shipping = bigquery.Dataset(f"{project_id}.shipping")
    shipping.location = "US"
    shipping.labels = {"department": "shipping", "active": "true"}
    bq_client.create_dataset(shipping)

    filtered = list(
        bq_client.list_datasets(filter="labels.department:receiving labels.active")
    )
    assert [dataset.dataset_id for dataset in filtered] == ["receiving"]

    metadata = bq_client.get_dataset(
        receiving.reference, dataset_view=bigquery.enums.DatasetView.METADATA
    )
    assert metadata.dataset_id == "receiving"
    assert metadata.labels == {"department": "receiving", "active": "true"}
