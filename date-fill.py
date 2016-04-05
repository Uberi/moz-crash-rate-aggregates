import os
from datetime import date, timedelta

# tiny utility that copies one day of crash aggregate data to 100 days into the past
# useful for testing with large amounts of data

ref_date = date(2016, 3, 16)

for i in range(1, 100):
    new_date = ref_date - timedelta(days=i)
    os.system(
        "aws s3 cp --recursive "
        "s3://telemetry-test-bucket/crash_aggregates/v1/submission_date={from_date} "
        "s3://telemetry-test-bucket/crash_aggregates/v1/submission_date={to_date}".format(
            from_date = ref_date.strftime("%Y-%m-%d"),
            to_date = new_date.strftime("%Y-%m-%d"),
        )
    )
