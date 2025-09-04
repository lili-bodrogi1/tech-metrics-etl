import great_expectations as gx

print("GE version:", gx.__version__)

context = gx.get_context()
print("Context type:", type(context))

# FluentDatasource létrehozása Snowflake-hez
datasource = context.sources.add_snowflake(
    name="snowflake_metrics",
    connection_string="snowflake://<USER>:<PASSWORD>@<ACCOUNT>/<DB>/<SCHEMA>?warehouse=<WAREHOUSE>&role=<ROLE>"
)

# Táblához hozzáférés
asset = datasource.add_table_asset(
    name="metrics_stg",
    table_name="METRICS",
    schema_name="STG"
)

# Batch request a táblára
batch_request = asset.build_batch_request()

# Validator létrehozás
validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name="metrics_suite"
)

# Példa expectation
validator.expect_column_values_to_not_be_null("TECHNOLOGY")
validator.expect_column_values_to_be_between("VALUE", min_value=0)

# Eredmény mentés
context.save_expectation_suite(validator.get_expectation_suite())

results = context.run_validation_operator(
    "action_list_operator",
    validators_to_validate=[validator] # Itt a validator object-nek kell lennie
)

print(results.success)  # True / False