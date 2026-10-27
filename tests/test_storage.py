from types import SimpleNamespace

from chronoflux.storage import BigQueryStore


def test_merge_updates_columns_by_name():
    class Job:
        def result(self):
            return None

    class Client:
        def __init__(self):
            self.queries = []

        def get_table(self, _name):
            return SimpleNamespace(schema=[
                SimpleNamespace(name=name)
                for name in ("gitlab_host", "pipeline_id", "status", "ingested_at")
            ])

        def create_table(self, _table):
            pass

        def load_table_from_json(self, _rows, _table, job_config):
            return Job()

        def query(self, sql):
            self.queries.append(sql)
            return Job()

        def delete_table(self, _table, not_found_ok=False):
            pass

    store = object.__new__(BigQueryStore)
    store.project = "test-project"
    store.dataset = "test_dataset"
    store.prefix = "`test-project.test_dataset."
    store.client = Client()
    store.bigquery = SimpleNamespace(
        Table=lambda name, schema: SimpleNamespace(name=name, schema=schema),
        LoadJobConfig=lambda **kwargs: SimpleNamespace(**kwargs),
    )

    store.merge("gitlab_pipelines", [{
        "gitlab_host": "gitlab.com", "pipeline_id": 12, "status": "success",
    }])

    assert "status=S.status" in store.client.queries[0]
    assert "ingested_at=S.ingested_at" in store.client.queries[0]
