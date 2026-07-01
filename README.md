# dagster_tutorial

## Getting started

### Installing dependencies

**Option 1: uv**

Ensure [`uv`](https://docs.astral.sh/uv/) is installed following their [official documentation](https://docs.astral.sh/uv/getting-started/installation/).

Create a virtual environment, and install the required dependencies using _sync_:

```bash
uv sync
```

Then, activate the virtual environment:

| OS | Command |
| --- | --- |
| MacOS | ```source .venv/bin/activate``` |
| Windows | ```.venv\Scripts\activate``` |

**Option 2: pip**

Install the python dependencies with [pip](https://pypi.org/project/pip/):

```bash
python3 -m venv .venv
```

Then activate the virtual environment:

| OS | Command |
| --- | --- |
| MacOS | ```source .venv/bin/activate``` |
| Windows | ```.venv\Scripts\activate``` |

Install the required dependencies:

```bash
pip install -e ".[dev]"
```

### Running Dagster

Start the Dagster UI web server:

```bash
dg dev
```

Open http://localhost:3000 in your browser to see the project.

## Pipeline overview

This project demonstrates a simple layered data pipeline in Dagster:

- Bronze layer: raw CSV files are ingested into DuckDB tables using the custom Tutorial component.
- Silver layer: the raw bronze tables are cleaned and standardized into silver tables such as `customers_silver`, `orders_silver`, and `payments_silver`.
- Gold layer: the silver tables are combined into the analytical table `orders_aggregation`.

The component-based ingestion lives in `src/dagster_tutorial/components/tutorial.py`, while the transformation logic for silver and gold assets lives in `src/dagster_tutorial/defs/asset.py`.

## Project structure

- `src/dagster_tutorial/components/tutorial.py` defines the reusable component that loads the bronze tables.
- `src/dagster_tutorial/defs/tutorial/defs.yaml` configures the bronze ingestion steps.
- `src/dagster_tutorial/defs/asset.py` contains the silver and gold asset definitions.
- `src/dagster_tutorial/definitions.py` wires everything together into a single Dagster Definitions object.

## Learn more

To learn more about this template and Dagster in general:

- [Dagster Documentation](https://docs.dagster.io/)
- [Dagster University](https://courses.dagster.io/)
- [Dagster Slack Community](https://dagster.io/slack)
