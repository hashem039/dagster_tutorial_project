from dagster_duckdb import DuckDBResource

import dagster as dg


def _run_sql(duckdb: DuckDBResource, sql: str) -> None:
    with duckdb.get_connection() as conn:
        conn.execute(sql)


@dg.asset(
    deps=["customers"],
    group_name="silver",
    tags={"layer": "silver"},
)
def customers_silver(duckdb: DuckDBResource):
    _run_sql(
        duckdb,
        """
        create or replace table customers_silver as (
            select
                cast(id as integer) as id,
                trim(first_name) as first_name,
                trim(last_name) as last_name
            from customers
        )
        """,
    )


@dg.asset(
    deps=["orders"],
    group_name="silver",
    tags={"layer": "silver"},
)
def orders_silver(duckdb: DuckDBResource):
    _run_sql(
        duckdb,
        """
        create or replace table orders_silver as (
            select
                cast(id as integer) as id,
                cast(user_id as integer) as user_id,
                cast(order_date as date) as order_date,
                trim(status) as status
            from orders
        )
        """,
    )


@dg.asset(
    deps=["payments"],
    group_name="silver",
    tags={"layer": "silver"},
)
def payments_silver(duckdb: DuckDBResource):
    _run_sql(
        duckdb,
        """
        create or replace table payments_silver as (
            select
                cast(id as integer) as id,
                cast(order_id as integer) as order_id,
                trim(payment_method) as payment_method,
                cast(amount as numeric(10, 2)) as amount
            from payments
        )
        """,
    )


@dg.asset(
    deps=["customers_silver", "orders_silver", "payments_silver"],
    group_name="gold",
    tags={"layer": "gold"},
)
def orders_aggregation(duckdb: DuckDBResource):
    _run_sql(
        duckdb,
        """
        create or replace table orders_aggregation as (
            select
                c.id as customer_id,
                c.first_name,
                c.last_name,
                count(distinct o.id) as total_orders,
                count(distinct p.id) as total_payments,
                coalesce(sum(p.amount), 0) as total_amount_spent
            from customers_silver c
            left join orders_silver o
                on c.id = o.user_id
            left join payments_silver p
                on o.id = p.order_id
            group by 1, 2, 3
        )
        """,
    )


@dg.asset_check(asset="orders_aggregation")
def orders_aggregation_check(duckdb: DuckDBResource) -> dg.AssetCheckResult:
    with duckdb.get_connection() as conn:
        row_count = conn.execute("select count(*) from orders_aggregation").fetchone()[0]

    if row_count == 0:
        return dg.AssetCheckResult(
            passed=False, metadata={"message": "Order aggregation check failed"}
        )

    return dg.AssetCheckResult(
        passed=True, metadata={"message": "Order aggregation check passed"}
    )
