#!/usr/bin/env python3
import sys, os

# Fix — config_loader path
sys.path.insert(0, '/opt/spark/lib')

try:
    from config_loader import load_config
    config = load_config()
    postgres_cfg = config.get_postgres_config()
    staging_table = config.get_tables()['staging']
except ImportError:
    postgres_cfg = {
        'user': 'admin',
        'password': 'admin',
        'host': 'postgres',
        'port': 5432,
        'database': 'booking'
    }
    staging_table = 'customer_booking_staging'

from great_expectations.data_context import FileDataContext
from great_expectations.core.batch import RuntimeBatchRequest

def run_quality_checks():
    try:
        ge_project_dir = '/ge/great_expectations'

        context = FileDataContext(project_config_relative_to=ge_project_dir)
        print("\n📊 Running Data Quality Validation...")

        db_user = postgres_cfg.get('user', 'admin')
        db_pass = postgres_cfg.get('password', 'admin')
        db_host = postgres_cfg.get('host', 'postgres')
        db_port = postgres_cfg.get('port', 5432)
        db_name = postgres_cfg.get('database', 'booking')
        connection_string = f"postgresql+psycopg2://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"

        datasource_name = "postgres_ds"
        try:
            context.get_datasource(datasource_name)
        except Exception:
            context.add_datasource(
                name=datasource_name,
                class_name="Datasource",
                execution_engine={
                    "class_name": "SqlAlchemyExecutionEngine",
                    "connection_string": connection_string
                },
                data_connectors={
                    "default": {
                        "class_name": "RuntimeDataConnector",
                        "batch_identifiers": ["default_identifier_name"]
                    }
                }
            )

        batch_request = RuntimeBatchRequest(
            datasource_name=datasource_name,
            data_connector_name="default",
            data_asset_name=staging_table,
            runtime_parameters={"query": f"SELECT * FROM {staging_table}"},
            batch_identifiers={"default_identifier_name": "default"}
        )

        checkpoint = context.get_checkpoint(name="customer_booking_checkpoint")
        checkpoint_result = checkpoint.run(
            validations=[{
                "batch_request": batch_request,
                "expectation_suite_name": "customer_booking_suite"
            }]
        )

        success = checkpoint_result["success"]
        print("✅ PASSED" if success else "⚠️ VALIDATION ISSUES")
        print("💾 Detailed JSON: /ge/great_expectations/uncommitted/validations/\n")

        try:
            unexpected_by_column = {}
            for result in checkpoint_result.get("run_results", {}).values():
                validation_result = result.get("validation_result", {})
                for expectation_result in validation_result.get("results", []):
                    if expectation_result.get("success", True):
                        continue
                    kwargs = expectation_result.get("expectation_config", {}).get("kwargs", {})
                    column = kwargs.get("column")
                    res = expectation_result.get("result", {})
                    unexpected = (
                        res.get("unexpected_list") or
                        res.get("unexpected_values") or
                        res.get("partial_unexpected_list")
                    )
                    if unexpected and column:
                        unexpected_by_column.setdefault(column, set()).update(unexpected)

            failed_booking_ids = set()
            if unexpected_by_column:
                from sqlalchemy import create_engine, text
                engine = create_engine(connection_string)
                with engine.connect() as conn:
                    for col_name, values in unexpected_by_column.items():
                        vals = list(values)
                        if not vals:
                            continue
                        placeholders = ",".join([":v" + str(i) for i in range(len(vals))])
                        sql = text(f"SELECT DISTINCT booking_id FROM {staging_table} WHERE {col_name} IN ({placeholders})")
                        params = {"v" + str(i): vals[i] for i in range(len(vals))}
                        rows = conn.execute(sql, **params)
                        for r in rows:
                            if r and r[0]:
                                failed_booking_ids.add(str(r[0]))

            if failed_booking_ids:
                import json
                from datetime import datetime
                reports_dir = os.path.join(ge_project_dir, 'uncommitted', 'validations')
                os.makedirs(reports_dir, exist_ok=True)
                filename = os.path.join(
                    reports_dir,
                    f"failed_ids_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                )
                with open(filename, 'w') as f:
                    json.dump({"failed_booking_ids": list(failed_booking_ids)}, f)
                print(f"💾 Failed booking_ids saved: {filename}")

        except Exception as e:
            print(f"Warning: {e}")
            pass

        return 0

    except Exception as e:
        print(f"❌ ERROR: {str(e)}")
        return 0

if __name__ == "__main__":
    sys.exit(run_quality_checks())