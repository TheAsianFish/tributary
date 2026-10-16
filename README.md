# Tributary

Streaming data platform with an evaluated analytics agent.

Tributary ingests an event stream through Kafka into a PostgreSQL warehouse
with Spark Structured Streaming, models it with dbt under Airflow with
data-quality checks, serves it through FastAPI, and answers questions about
it in plain English with a text-to-SQL agent that is graded on BIRD Mini-Dev
(execution accuracy, PostgreSQL) and guarded against unsafe queries. The demo
data is the live Wikipedia edit stream; one `docker compose up` brings
everything up with a committed sample.

**Status: in progress.** See [PLAN.md](PLAN.md) for the design and
milestones and [CONTRIBUTING.md](CONTRIBUTING.md) for setup and commands.
Results will appear in RESULTS.md as they are measured.

## Licence

MIT. Benchmark data (BIRD Mini-Dev, CC BY-SA 4.0) is downloaded on demand
and never committed.
