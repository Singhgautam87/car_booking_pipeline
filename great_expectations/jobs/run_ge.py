#!/usr/bin/env python3
import sys, os
import json
from datetime import datetime

sys.path.insert(0, '/opt/spark/lib')

try:
    from config_loader import load_config
    config = load_config()
    postgres_cfg = config.get_postgres_config()
    mysql_cfg = config.get_mysql_config()
    staging_table = config.get_tables()['staging']
    validation_table = config.get_tables()['validation_results']
except ImportError:
    postgres_cfg = {
        'user': 'admin', 'password': 'admin',
        'host': 'postgres', 'port': 5432, 'database': 'booking'
    }
    mysql_cfg = {
        'user': 'admin', 'password': 'admin',
        'host': 'mysql', 'port': 3306, 'database': 'booking'
    }
    staging_table = 'customer_booking_staging'
    validation_table = 'validation_results'

import great_expectations as gx


def save_to_mysql(mysql_cfg, validation_table, run_id, total, passed, failed, rate, status, success, results_list):
    try:
        import mysql.connector
        conn = mysql.connector.connect(
            host=mysql_cfg.get('host', 'mysql'),
            user=mysql_cfg.get('user', 'admin'),
            password=mysql_cfg.get('password', 'admin'),
            database=mysql_cfg.get('database', 'booking')
        )
        cursor = conn.cursor()

        cursor.execute(f"""
            INSERT IGNORE INTO {validation_table}
            (run_identifier, checkpoint_name, total_expectations,
             passed_expectations, failed_expectations, success_rate,
             validation_status, validation_details)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            run_id, "customer_booking_checkpoint",
            total, passed, failed, round(rate, 2), status,
            json.dumps({"success": success, "total": total, "passed": passed, "failed": failed})
        ))

        for r in results_list:
            exp_type    = r.get("expectation_type", "unknown")
            kwargs      = r.get("kwargs", {})
            result      = r.get("result", {})
            exp_success = r.get("success", False)

            column_name  = kwargs.get("column", "table_level")
            batch_id     = kwargs.get("batch_id", "")
            min_value    = str(kwargs.get("min_value", "")) if kwargs.get("min_value") is not None else None
            max_value    = str(kwargs.get("max_value", "")) if kwargs.get("max_value") is not None else None
            regex        = kwargs.get("regex", None)
            value_set    = json.dumps(kwargs.get("value_set", [])) if kwargs.get("value_set") else None

            observed_value                = str(result.get("observed_value", "")) if result.get("observed_value") is not None else None
            element_count                 = result.get("element_count", 0) or 0
            unexpected_count              = result.get("unexpected_count", 0) or 0
            unexpected_percent            = result.get("unexpected_percent", 0.0) or 0.0
            missing_count                 = result.get("missing_count", 0) or 0
            missing_percent               = result.get("missing_percent", 0.0) or 0.0
            unexpected_percent_total      = result.get("unexpected_percent_total", 0.0) or 0.0
            unexpected_percent_nonmissing = result.get("unexpected_percent_nonmissing", 0.0) or 0.0
            partial_unexpected_list       = json.dumps(result.get("partial_unexpected_list", []))
            partial_unexpected_counts     = json.dumps(result.get("partial_unexpected_counts", []))

            cursor.execute("""
                INSERT INTO validation_expectation_details
                (run_identifier, expectation_type, column_name, batch_id, success,
                 observed_value, element_count, unexpected_count, unexpected_percent,
                 missing_count, missing_percent, unexpected_percent_total,
                 unexpected_percent_nonmissing, partial_unexpected_list,
                 partial_unexpected_counts, min_value, max_value, regex, value_set)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (
                run_id, exp_type, column_name, batch_id, exp_success,
                observed_value, element_count, unexpected_count, round(unexpected_percent, 4),
                missing_count, missing_percent, round(unexpected_percent_total, 4),
                round(unexpected_percent_nonmissing, 4), partial_unexpected_list,
                partial_unexpected_counts, min_value, max_value, regex, value_set
            ))

        conn.commit()
        cursor.close()
        conn.close()
        print(f"✅ validation_results + validation_expectation_details save hua!")
        print(f"   Run: {run_id} | Total: {total} | Passed: {passed} | Failed: {failed}")

    except Exception as e:
        print(f"⚠️ Warning MySQL save: {e}")
        import traceback
        traceback.print_exc()


def save_failed_records(mysql_cfg, postgres_cfg, staging_table, run_id, results_list):
    try:
        from sqlalchemy import create_engine, text
        import mysql.connector

        pg_engine = create_engine(
            f"postgresql+psycopg2://{postgres_cfg['user']}:{postgres_cfg['password']}"
            f"@{postgres_cfg['host']}:{postgres_cfg['port']}/{postgres_cfg['database']}"
        )

        mysql_conn = mysql.connector.connect(
            host=mysql_cfg.get('host', 'mysql'),
            user=mysql_cfg.get('user', 'admin'),
            password=mysql_cfg.get('password', 'admin'),
            database=mysql_cfg.get('database', 'booking')
        )
        cursor = mysql_conn.cursor(dictionary=True)

        total_inserted = 0

        for r in results_list:
            if r.get("success", True):
                continue

            exp_type    = r.get("expectation_type", "unknown")
            kwargs      = r.get("kwargs", {})
            column_name = kwargs.get("column", "table_level")

            if "unique" in exp_type:
                failed_reason = "Duplicate value found"
                pg_query = text(f"""
                    SELECT * FROM {staging_table}
                    WHERE booking_id IN (
                        SELECT booking_id FROM {staging_table}
                        GROUP BY booking_id HAVING COUNT(*) > 1
                    )
                """)
                params = {}

            elif "null" in exp_type:
                failed_reason = "Null value found"
                pg_query = text(f"""
                    SELECT * FROM {staging_table}
                    WHERE {column_name} IS NULL
                """)
                params = {}

            elif "in_set" in exp_type:
                value_set = kwargs.get("value_set", [])
                failed_reason = f"Value not in set {value_set}"
                placeholders = ",".join([f":v{i}" for i in range(len(value_set))])
                pg_query = text(f"""
                    SELECT * FROM {staging_table}
                    WHERE {column_name} NOT IN ({placeholders})
                """)
                params = {f"v{i}": value_set[i] for i in range(len(value_set))}

            elif "regex" in exp_type:
                regex = kwargs.get("regex", "")
                failed_reason = f"Regex mismatch: {regex}"
                pg_query = text(f"""
                    SELECT * FROM {staging_table}
                    WHERE {column_name} !~ :regex
                """)
                params = {"regex": regex}

            elif "between" in exp_type:
                min_val = kwargs.get("min_value")
                max_val = kwargs.get("max_value")
                failed_reason = f"Value out of range [{min_val} - {max_val}]"
                if min_val is not None and max_val is not None:
                    pg_query = text(f"""
                        SELECT * FROM {staging_table}
                        WHERE {column_name} < :min_val OR {column_name} > :max_val
                    """)
                    params = {"min_val": min_val, "max_val": max_val}
                elif min_val is not None:
                    pg_query = text(f"""
                        SELECT * FROM {staging_table}
                        WHERE {column_name} < :min_val
                    """)
                    params = {"min_val": min_val}
                else:
                    continue
            else:
                continue

            # ✅ PostgreSQL staging se poore failed records lo
            with pg_engine.connect() as pg_conn:
                result = pg_conn.execute(pg_query, params)
                rows = result.mappings().all()

            if not rows:
                print(f"   → {exp_type} | {column_name} | No failed records")
                continue

            print(f"   → {exp_type} | {column_name} | Failed records: {len(rows)}")

            for row in rows:
                try:
                    cursor.execute("""
                        INSERT INTO validation_failed_records
                        (run_identifier, expectation_type, column_name,
                         failed_reason, failed_value,
                         booking_id, customer_id, customer_name, email,
                         loyalty_tier, loyalty_points, booking_date,
                         pickup_location, drop_location, car_id, model,
                         price_per_day, insurance_provider, insurance_coverage,
                         payment_id, payment_method, payment_amount)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """, (
                        run_id,
                        exp_type,
                        column_name,
                        failed_reason,
                        str(row.get(column_name, '')),
                        row.get('booking_id'),
                        row.get('customer_id'),
                        row.get('customer_name'),
                        row.get('email'),
                        row.get('loyalty_tier'),
                        row.get('loyalty_points'),
                        row.get('booking_date'),
                        row.get('pickup_location'),
                        row.get('drop_location'),
                        row.get('car_id'),
                        row.get('model'),
                        row.get('price_per_day'),
                        row.get('insurance_provider'),
                        row.get('insurance_coverage'),
                        row.get('payment_id'),
                        row.get('payment_method'),
                        row.get('payment_amount')
                    ))
                    total_inserted += 1
                except Exception as row_err:
                    print(f"   ⚠️ Row insert error: {row_err}")
                    continue

        mysql_conn.commit()
        cursor.close()
        mysql_conn.close()
        pg_engine.dispose()
        print(f"✅ validation_failed_records save hua! Total: {total_inserted}")

    except Exception as e:
        print(f"⚠️ Warning failed records: {e}")
        import traceback
        traceback.print_exc()


def load_suite_from_file(context, ge_project_dir):
    suite_path = os.path.join(ge_project_dir, 'expectations', 'customer_booking_suite.json')
    with open(suite_path, 'r') as f:
        suite_data = json.load(f)

    expectations = []
    for exp in suite_data.get("expectations", []):
        exp_type = exp["type"]
        kwargs = exp["kwargs"]
        class_name = "".join(word.capitalize() for word in exp_type.split("_"))
        try:
            exp_class = getattr(gx.expectations, class_name)
            expectations.append(exp_class(**kwargs))
        except Exception as e:
            print(f"⚠️ Skipping {exp_type}: {e}")

    suite = context.suites.add_or_update(
        gx.ExpectationSuite(
            name="customer_booking_suite",
            expectations=expectations
        )
    )
    print(f"✅ Suite loaded: {len(suite.expectations)} expectations")
    return suite


def run_quality_checks():
    try:
        ge_project_dir = '/ge/great_expectations'
        context = gx.get_context(project_root_dir='/ge')

        print("\n📊 Running Data Quality Validation...")

        db_user = postgres_cfg.get('user', 'admin')
        db_pass = postgres_cfg.get('password', 'admin')
        db_host = postgres_cfg.get('host', 'postgres')
        db_port = postgres_cfg.get('port', 5432)
        db_name = postgres_cfg.get('database', 'booking')
        connection_string = f"postgresql+psycopg2://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"

        datasource_name = "postgres_ds"

        datasource = context.data_sources.add_or_update_postgres(
            name=datasource_name,
            connection_string=connection_string
        )

        try:
            asset = datasource.get_asset(staging_table)
        except Exception:
            asset = datasource.add_table_asset(
                name=staging_table,
                table_name=staging_table
            )

        batch_definition = asset.add_batch_definition_whole_table("whole_table")
        suite = load_suite_from_file(context, ge_project_dir)

        try:
            validation_definition = context.validation_definitions.get("customer_booking_validation")
        except Exception:
            validation_definition = context.validation_definitions.add(
                gx.ValidationDefinition(
                    name="customer_booking_validation",
                    data=batch_definition,
                    suite=suite
                )
            )

        validation_result = validation_definition.run()
        success = validation_result["success"]
        print("✅ PASSED" if success else "⚠️ VALIDATION ISSUES")

        # ✅ JSON save
        reports_dir = os.path.join(ge_project_dir, 'uncommitted', 'validations')
        os.makedirs(reports_dir, exist_ok=True)
        result_filename = os.path.join(
            reports_dir,
            f"validation_result_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        )
        result_dict = validation_result.describe_dict()
        with open(result_filename, 'w') as f:
            json.dump(result_dict, f)
        print(f"💾 JSON saved: {result_filename}")

        # ✅ Stats
        stats = result_dict.get("statistics", {})
        total  = stats.get("evaluated_expectations", 0)
        passed = stats.get("successful_expectations", 0)
        failed = stats.get("unsuccessful_expectations", 0)
        rate   = stats.get("success_percent", 0.0) or 0.0
        status = "PASSED" if success else "FAILED"
        run_id = datetime.now().strftime('%Y%m%d_%H%M%S')
        results_list = result_dict.get("expectations", [])

        print(f"📊 Total: {total} | Passed: {passed} | Failed: {failed} | Rate: {rate}%")

        # ✅ MySQL mein save karo
        save_to_mysql(mysql_cfg, validation_table, run_id, total, passed, failed, rate, status, success, results_list)

        # ✅ Failed records — PostgreSQL staging se directly
        save_failed_records(mysql_cfg, postgres_cfg, staging_table, run_id, results_list)

        return 0

    except Exception as e:
        print(f"❌ ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        return 0


if __name__ == "__main__":
    sys.exit(run_quality_checks())
