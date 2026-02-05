import sys
import yaml
from great_expectations.data_context import BaseDataContext

# Create GE context (no init needed)
context = BaseDataContext(project_config=None)

# Add Postgres datasource
context.add_datasource(
    name="postgres_ds",
    class_name="Datasource",
    execution_engine={
        "class_name": "SqlAlchemyExecutionEngine",
        "connection_string": "postgresql://admin:admin@postgres:5432/booking"
    },
    data_connectors={
        "runtime_connector": {
            "class_name": "RuntimeDataConnector",
            "batch_identifiers": ["default_identifier"]
        }
    }
)

# Load checkpoint YAML
with open("/ge/checkpoints/customer_booking_checkpoint.yml") as f:
    checkpoint_config = yaml.safe_load(f)

# Run checkpoint
results = context.run_checkpoint(**checkpoint_config)

# Fail pipeline if data quality fails
if not results["success"]:
    print("❌ DATA QUALITY FAILED")
    sys.exit(1)

print("✅ DATA QUALITY PASSED")
