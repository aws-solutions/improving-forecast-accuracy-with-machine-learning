# #####################################################################################################################
#  Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.                                            #
#                                                                                                                     #
#  Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance     #
#  with the License. A copy of the License is located at                                                              #
#                                                                                                                     #
#  http://www.apache.org/licenses/LICENSE-2.0                                                                         #
#                                                                                                                     #
#  or in the 'license' file accompanying this file. This file is distributed on an 'AS IS' BASIS, WITHOUT WARRANTIES  #
#  OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions     #
#  and limitations under the License.                                                                                 #
# #####################################################################################################################

import csv
import random
from datetime import datetime, timezone, timedelta

import click
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd

years = mdates.YearLocator()
months = mdates.MonthLocator()
years_fmt = mdates.DateFormatter("%Y")


@click.command()
def create():
    """This creates some synthetic demand data"""
    start = datetime(2000, 1, 1, tzinfo=timezone.utc)
    starting_demand = 100.0
    items = 1
    weekly_sales_adjustment = [0.9, 1, 1.1, 1.2, 1.25, 1.1, 0.7]
    monthly_sales_adjustment = [0.9, 0.95, 1, 1, 1, 1, 1.1, 1.1, 1.1, 1, 1, 1.1, 1.25]

    # as price goes up, demand goes down
    price = 10.00

    demand = starting_demand
    time = start
    with open("demand.related.csv", "w") as demand_related_file:
        with open("demand.csv", "w") as demand_file:
            demand_writer = csv.writer(demand_file)
            demand_related_writer = csv.writer(demand_related_file)
            for i in range(0, 365 * 15):
                for item in range(items):
                    random_demand = demand

                    # apply randomness
                    random_demand = random_demand + random.uniform(
                        -demand / 10, demand / 10
                    )
                    random_demand = (
                        random_demand * weekly_sales_adjustment[time.weekday()]
                    )
                    random_demand = random_demand * monthly_sales_adjustment[time.month]

                    # write it
                    row = f"item_{item:03},{time:%Y-%m-%d},{random_demand:.3f}"
                    demand_writer.writerow(row.split(","))

                    row = f"item_{item:03},{time:%Y-%m-%d},{price}"
                    demand_related_writer.writerow(row.split(","))

                    # next values
                    demand = demand + 0.03

                    time = time + timedelta(days=1)

                    # adjust price. this seller is attempting to
                    # get demand to 150 units, and keep it there
                    # by adjusting their price.
                    # price adjustments are made approx. weekly
                    #
                    # buyers are sensitive to price increases
                    # buyers are not as sensitive to price decreases
                    if i % 7 == 0:
                        if random_demand < 150:
                            if price > 5.0:
                                price = price - 0.5
                                demand = demand + 0.05
                        else:
                            if price < 20.0:
                                # increasing the price has an impact on demand
                                price = price + 0.5
                                demand = demand - 0.5

    with open("demand.metadata.csv", "w") as metadata_file:
        metadata_file.write(f"item_000,category_a,brand_a")

    timeseries = pd.read_csv("demand.csv", header=None, parse_dates=[1])
    related = pd.read_csv("demand.related.csv", header=None, parse_dates=[1])

    fig, (ax, bx) = plt.subplots(2, sharex=True)
    ax.set_title("Item Demand")
    ax.set_xlabel("Date")
    ax.set_ylabel("Demand")
    ax.plot(timeseries[1], timeseries[2])

    ax.xaxis.set_major_locator(years)
    ax.xaxis.set_major_formatter(years_fmt)
    ax.xaxis.set_minor_locator(months)

    ax.format_xdata = mdates.DateFormatter("%Y-%m-%d")
    ax.grid(True)

    bx.set_title("Item Price")
    bx.set_xlabel("Date")
    bx.set_ylabel("Price ($)")
    bx.plot(related[1], related[2])
    bx.grid(True)

    fig.autofmt_xdate()

    plt.show()


if __name__ == "__main__":
    create()
