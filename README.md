ETL Stream

Example SQL :
INSERT INTO sources (name, pg_host, pg_port, pg_database, pg_username, pg_password, publication_name)
VALUES ('test_source', 'localhost', 5433, 'postgres', 'postgres', 'postgres', 'my_publication');

INSERT INTO destinations (name, destination_type, config)
VALUES ('test_http', 'http', '{"url": "http://localhost:5001"}');

INSERT INTO pipelines (name, source_id, destination_id, status)
VALUES ('test_pipeline', 1, 1, 'START');