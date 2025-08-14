from dagster import Definitions, load_assets_from_modules,ScheduleDefinition, define_asset_job

from news_etl_analytics import assets  # noqa: TID252
from . import assets

# Create a job from all assets
etl_job = define_asset_job("etl_job", selection="*")

etl_schedule = ScheduleDefinition(
    job=etl_job,
    cron_schedule="30 10-22 * * *",  # Valid cron expression
    execution_timezone="Asia/Kolkata"
)


defs = Definitions(
    assets=[assets.bronze, assets.silver, assets.gold],
    schedules=[etl_schedule],
)
