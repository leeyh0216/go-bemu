# bqemu:operation bigquery.jobs.query.parameters scenario=query-parameters
from google.cloud import bigquery


def test_official_client_binds_named_and_positional_parameters(bq_client: bigquery.Client) -> None:
    named = bq_client.query(
        "SELECT @number AS number, @text AS text",
        job_config=bigquery.QueryJobConfig(
            priority=bigquery.QueryPriority.BATCH,
            labels={"consumer": "python"},
            query_parameters=[
                bigquery.ScalarQueryParameter("number", "INT64", 42),
                bigquery.ScalarQueryParameter("text", "STRING", "bound value"),
            ],
        ),
    )
    rows = list(named.result())
    assert rows[0]["number"] == 42
    assert rows[0]["text"] == "bound value"

    positional = bq_client.query(
        "SELECT ? + ? AS total",
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(None, "INT64", 2),
                bigquery.ScalarQueryParameter(None, "INT64", 3),
            ],
        ),
    )
    assert list(positional.result())[0]["total"] == 5
