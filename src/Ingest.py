from __future__ import division
import datetime
import pandas as pd


def ingest(e, D):
    data = pd.read_json(e)
    data.assign(yearweek='')

    for x in range(0, len(data)):
        year = datetime.datetime.strptime(
            data.event_time[x], '%Y-%m-%d:%H:%M:%S.%fZ').isocalendar()[0]
        week = datetime.datetime.strptime(
            data.event_time[x], '%Y-%m-%d:%H:%M:%S.%fZ').isocalendar()[1]
        data.at[x, 'yearweek'] = f'{year}_{week}'

    # Parse the JSON string by type
    customers = data[data['type'] == 'CUSTOMER'].copy()
    print ("Customer Count ", len(customers))
    visits = data[data['type'] == 'SITE_VISIT'].copy()
    images = data[data['type'] == 'IMAGE'].copy()
    orders = data[data['type'] == 'ORDER'].copy()
    orders['total_amount'] = orders['total_amount'].str.replace(' USD', '').astype(float)

    # Expenditures by customer into DataFrame
    epw = pd.DataFrame(orders.groupby(['customer_id'])[['total_amount']].sum())

    # Visits per week by customer into DataFrame
    vpw = pd.DataFrame(visits.groupby(['customer_id']).size())

    for r in epw.iterrows():
        if D[D.customer_id != r[0]].any:
            if r[0] in vpw.index.values and r[0] in epw.index.values:
                D.loc[len(D)] = [r[0], vpw.loc[r[0]].iloc[0] * epw.loc[r[0]].iloc[0] * 52 * 10]

    return D


def TopXSimpleLTVCustomers(x, D):
    top_x = D.nlargest(x, 'LTV')
    return top_x


# Testing
DATA = pd.DataFrame(columns=['customer_id', 'LTV'])
DATA = ingest('https://raw.githubusercontent.com/jmagana2000/code-challenge/master/input/events.txt', DATA)
top = TopXSimpleLTVCustomers(10, DATA)
print(top)
