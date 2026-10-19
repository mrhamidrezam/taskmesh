import sys
import time
from pathlib import Path

# Add project root to sys.path for direct execution without installation
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from taskmesh import DAG, Engine, ErrorPolicy


def main():
    # 1. Initialize DAG
    dag = DAG(
        name="daily_analytics_flow",
        description="Daily report generator with data collection, transformation, and alerting.",
    )

    # 2. Define tasks using decorators
    @dag.task(description="Extract source database records")
    def extract_db(ctx):
        print("  [DB] Extracting raw records...")
        time.sleep(0.05)
        return [{"product": "Widget A", "sales": 150}, {"product": "Widget B", "sales": 320}]

    @dag.task(description="Extract API third-party analytics")
    def extract_api(ctx):
        print("  [API] Fetching remote analytics...")
        time.sleep(0.05)
        return {"traffic_visits": 12500, "conversion_rate": 0.038}

    @dag.task(
        dependencies=["extract_db", "extract_api"],
        description="Compute combined business metrics",
    )
    def compute_kpis(ctx):
        db_data = ctx.get("extract_db")
        api_data = ctx.get("extract_api")
        total_sales = sum(item["sales"] for item in db_data)
        revenue_estimate = total_sales * 25.0
        return {
            "total_sales_units": total_sales,
            "revenue_estimate": revenue_estimate,
            "visits": api_data["traffic_visits"],
        }

    @dag.task(
        dependencies=["compute_kpis"],
        retries=2,
        retry_delay=0.1,
        description="Generate formatted report summary",
    )
    def format_summary(ctx):
        kpis = ctx.get("compute_kpis")
        return f"KPI Report: {kpis['total_sales_units']} units sold (${kpis['revenue_estimate']:,.2f}) across {kpis['visits']:,} visitors."

    # 3. Print DAG ASCII structure
    print("\n--- Pipeline Structure ---")
    print(dag.to_ascii())

    # 4. Execute pipeline concurrently
    print("\n--- Running Pipeline ---")
    engine = Engine(max_workers=4)
    result = engine.run(dag)

    # 5. Review results
    print("\n--- Execution Outcome ---")
    print(f"Overall Success: {result.success}")
    print(f"Total Duration:  {result.total_duration:.4f}s")
    print(f"Report:          {result.get('format_summary')}")


if __name__ == "__main__":
    main()
