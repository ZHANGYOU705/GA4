import os
import pandas as pd
import itertools
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Metric,
    RunReportRequest,
)

starting_date = "2023-08-01"
ending_date = "2023-08-07"
property_id = "401635008"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'zhangyou_ga4.json'

request_api = RunReportRequest(
    property=f"properties/{property_id}",
    dimensions=[
        Dimension(name="date")
    ],
    metrics=[
        Metric(name="totalUsers")
    ],
    date_ranges=[DateRange(start_date=starting_date, end_date=ending_date)],
)
client = BetaAnalyticsDataClient()
response = client.run_report(request_api)
print(response)


def query_data(api_response):
    dimension_headers = [header.name for header in api_response.dimension_headers]
    metric_headers = [header.name for header in api_response.metric_headers]
    dimensions = []
    metrics = []
    for i in range(len(dimension_headers)):
        dimensions.append([row.dimension_values[i].value for row in api_response.rows])

    for i in range(len(metric_headers)):
        metrics.append([row.metric_values[i].value for row in api_response.rows])
    headers = dimension_headers, metric_headers
    headers = list(itertools.chain.from_iterable(headers))
    data = dimensions, metrics
    data = list(itertools.chain.from_iterable(data))
    df = pd.DataFrame(data)
    df = df.transpose()
    df.columns = headers
    return df


final_data = query_data(response)
print(final_data)

final_data.to_csv('file.csv', index=False)
