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
        data.set_value(x, 'yearweek', '{0}_{1}'.format(year, week))

    # Parse the JSON string by type
    customers = data[data['type'] == 'CUSTOMER']
    print "Customer Count ", len(customers)
    visits = data[data['type'] == 'SITE_VISIT']
    images = data[data['type'] == 'IMAGE']
    orders = data[data['type'] == 'ORDER']

    # Strip 'USD' From total amount to sum amounts
    for k, v in orders['total_amount'].iteritems():
        orders.set_value(k, 'total_amount', float(str(v).split(' ')[0]))

    # Expenditures by customer into DataFrame
    epw = pd.DataFrame(orders.groupby(['customer_id'])[['total_amount']].sum())

    # Visits per week by customer into DataFrame
    vpw = pd.DataFrame(visits.groupby(['customer_id']).size())

    for r in epw.iterrows():
        if D[D.customer_id != r[0]].any:
            if r[0] in vpw.index.values and r[0] in epw.index.values:
                D.loc[len(D)] = [r[0], vpw.loc[r[0]][0]*epw.loc[r[0]][0]*52*10]

    return D


def TopXSimpleLTVCustomers(x, D):
    top_x = D.nlargest(x, 'LTV')
    return top_x


# Testing
DATA = pd.DataFrame(columns=['customer_id', 'LTV'])
DATA = ingest('https://raw.githubusercontent.com/jmagana2000/code-challenge/master/input/events.txt', DATA)
top = TopXSimpleLTVCustomers(10, DATA)
print top
