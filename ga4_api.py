#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import datetime
import os
import sys

import pandas as pd
import itertools

from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Metric,
    RunReportRequest,
)


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


company_properties = {
    1001: "401635008",
    1002: "521636172",
}

if __name__ == '__main__':

    if len(sys.argv) < 3:
        ending_date = datetime.datetime.now().strftime('%Y-%m-%d')
        starting_date = (
                datetime.datetime.strptime(ending_date, '%Y-%m-%d') - datetime.timedelta(
            days=1)).strftime('%Y-%m-%d')
    else:
        starting_date = sys.argv[1]
        ending_date = sys.argv[2]

    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'zhangyou_ga4.json'
    for company, property_id in company_properties.items():
        try:
            request_api = RunReportRequest(
                property=f"properties/{property_id}",
                dimensions=[
                    Dimension(name="date")
                ],
                metrics=[
                    Metric(name="totalUsers"),
                    Metric(name="transactions")
                ],
                date_ranges=[DateRange(start_date=starting_date, end_date=ending_date)],
            )
            client = BetaAnalyticsDataClient()
            response = client.run_report(request_api, timeout=60)

            result_data = query_data(response)

            print(f"Report for Company {company}:\n")
            print(result_data)
            print()
        except Exception as e:
            print(f"Error processing data for Company {company}: {str(e)}")
