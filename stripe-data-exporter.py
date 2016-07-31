#!/usr/bin/python

import argparse

import psycopg2
import stripe

parser = argparse.ArgumentParser(description='Export data from Stripe')
parser.add_argument('--fromInvoice', metavar='invoice',
                    help='From which invoice we will start')
parser.add_argument('--apiKey', metavar='key', required=True,
                    help='Stripe api key')
parser.add_argument('--db', metavar='db', help='SQL database name', default='stripe')
parser.add_argument('--user', metavar='user', help='SQL user', default='analytic')
parser.add_argument('--password', metavar='password', help='SQL password', default='')
parser.add_argument('--host', metavar='host', help='SQL host', default='localhost')

args = parser.parse_args()

conn = psycopg2.connect("dbname='{}' user='{}' host='{}' password='{}'".format(args.db, args.user, args.host, args.password))
cur = conn.cursor()

stripe.api_key = args.apiKey

if args.fromInvoice is None:
    cur.execute("TRUNCATE TABLE subscription")
    cur.execute("TRUNCATE TABLE line_item")
    cur.execute("TRUNCATE TABLE refund")
    cur.execute("TRUNCATE TABLE charge")
    # cur.execute("TRUNCATE TABLE plan")
    cur.execute("TRUNCATE TABLE invoice")
    conn.commit()


def map_func(value):
    if value is None:
        return "null"
    if isinstance(value, basestring):
        return "'{}'".format(str(value).replace("'", "''"))
    return str(value)


def add_to_sql(dict_to_add):
    object_type = dict_to_add.pop("object")
    dict_to_add.pop("metadata", None)
    placeholders = ', '.join(map(map_func, dict_to_add.values()))
    columns = ', '.join(dict_to_add.keys())
    sql = "INSERT INTO {0:s} ( {1:s} ) VALUES ( {2:s} )".format(object_type, columns, placeholders)
    try:
        cur.execute(sql)
    except:
        print("Error in query: {}".format(sql))
        raise


if args.fromInvoice is None:
    invoices = stripe.Invoice.list(limit=100, expand=['data.charge'])
else:
    invoices = stripe.Invoice.list(limit=100, expand=['data.charge'], starting_after=args.fromInvoice)

count = 0
last_invoice = ''
invoice = {}
try:
    for invoice in invoices.auto_paging_iter():

        charge = invoice.pop("charge", None)
        invoice.pop("payment")
        lines = invoice.pop("lines", None)
        discount = invoice.pop("discount", None)
        add_to_sql(invoice)

        if charge is not None:
            source = charge.pop("source", None)
            refunds = charge.pop("refunds", None)
            charge.pop("fraud_details", None)
            charge.pop("order", None)
            charge.pop("source_transfer", None)
            charge.pop("dispute", None)
            charge.pop("shipping", None)

            add_to_sql(charge)
            if refunds is not None:
                for refund in refunds:
                    add_to_sql(refund)

        if lines is not None:
            for line in lines:
                plan = line.pop("plan", None)
                if plan is not None:
                    line["plan"] = plan['id']
                    line['invoice'] = invoice['id']
                    if line['object'] == 'line_item':
                        period = line.pop("period")
                        line["period_start"] = period['start']
                        line["period_end"] = period['end']
                    add_to_sql(line)

        conn.commit()
        count += 1
        if count % 100 == 0:
            print("Progress: {}".format(count))

        last_invoice = invoice['id']
except:
    print("Failed at {} invoice".format(invoice['id']))
    print("Previous successful invoice: {}".format(last_invoice))
    raise
