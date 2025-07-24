# ml_pipeline.py
import toml
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, lit, when
import mlflow

# --- Configuration ---
DBT_MART_TABLE = "MART_SALES_WITH_FEATURES"
TRAINING_CUTOFF_DAY = 1913
FORECAST_DAYS = 28
LEAD_TIME_DAYS = 7
SERVICE_LEVEL_Z_SCORE = 1.645

# --- Feature and Output Configuration ---
FEATURE_COLS = [
    "SELL_PRICE", "SALES_LAG_28", "SALES_ROLLING_AVG_7",
    "SALES_ROLLING_STD_7", "SALES_ROLLING_AVG_28", "SALES_ROLLING_STD_28"
]
TARGET_COL = "TARGET_SALES"
ID_COLS = ["ID", "FULL_DATE", "DAY_NUM"]

# --- Snowflake Object Names ---
SNOWFLAKE_MODEL_NAME = "M5_XGB_FORECAST_MODEL"
FORECAST_OUTPUT_TABLE = "M5_DEMAND_FORECASTS"
INVENTORY_OUTPUT_TABLE = "M5_INVENTORY_RECOMMENDATIONS"

def main():
    """Main function to run the ML pipeline."""
    try:
        with open('config.toml', 'r') as f:
            connection_params = toml.load(f)['connection']
    except (FileNotFoundError, KeyError):
        print("Error: Ensure config.toml exists and has a [connection] section.")
        return

    session = Session.builder.configs(connection_params).create()
    print("Snowpark session created.")

    dbt_schema = connection_params['schema']
    features_df = session.table(f"{dbt_schema}.{DBT_MART_TABLE}")

    # Final data cleaning before sending to model
    # We fill nulls created by lag/rolling features
    cols_to_fill = FEATURE_COLS + [TARGET_COL]
    processed_df = features_df
    for c in cols_to_fill:
        processed_df = processed_df.with_column(c, when(col(c).is_null(), lit(0)).otherwise(col(c)))

    mlflow.set_tracking_uri("http://127.0.0.1:5000")
    mlflow.set_experiment("M5 Demand Forecasting")

    with mlflow.start_run() as run:
        print(f"Starting MLflow Run: {run.info.run_id}")
        mlflow.log_params({
            "training_cutoff_day": TRAINING_CUTOFF_DAY,
            "forecast_days": FORECAST_DAYS,
            "model_type": "XGBOOST_REGRESSOR",
            "features": ", ".join(FEATURE_COLS)
        })

        # 1. TRAIN THE MODEL
        print("Training model using Snowflake ML...")
        train_df = processed_df.filter(f"DAY_NUM <= {TRAINING_CUTOFF_DAY}")

        from snowflake.ml.modeling.xgboost import XGBRegressor
        
        regressor = XGBRegressor(input_cols=FEATURE_COLS, label_cols=TARGET_COL, output_cols="PREDICTED_SALES")
        regressor.fit(train_df)
        
        print(f"Model '{SNOWFLAKE_MODEL_NAME}' trained and saved.")

        # 2. MAKE PREDICTIONS
        print("Making predictions...")
        test_df = processed_df.filter(f"DAY_NUM > {TRAINING_CUTOFF_DAY} AND DAY_NUM <= {TRAINING_CUTOFF_DAY + FORECAST_DAYS}")
        
        forecast_results_df = regressor.predict(test_df)
        
        final_forecasts = forecast_results_df.select(*ID_COLS, "PREDICTED_SALES")
        final_forecasts.write.mode("overwrite").save_as_table(FORECAST_OUTPUT_TABLE)
        print(f"Forecasts saved to table '{FORECAST_OUTPUT_TABLE}'.")

        # 3. CALCULATE INVENTORY
        print("Calculating inventory recommendations...")
        inventory_df = final_forecasts.join(
            features_df.select("ID", "SALES_ROLLING_STD_28"), "ID", "left"
        ).group_by("ID").agg(
            {"PREDICTED_SALES": "sum", "SALES_ROLLING_STD_28": "avg"}
        ).select(
            "ID",
            (col("AVG(SALES_ROLLING_STD_28)") * SERVICE_LEVEL_Z_SCORE).alias("SAFETY_STOCK"),
            (col("SUM(PREDICTED_SALES)") + col("SAFETY_STOCK")).alias("REORDER_POINT")
        )

        inventory_df.write.mode("overwrite").save_as_table(INVENTORY_OUTPUT_TABLE)
        print(f"Inventory recommendations saved to table '{INVENTORY_OUTPUT_TABLE}'.")

    session.close()
    print("Pipeline finished successfully.")

if __name__ == '__main__':
    main()