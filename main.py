#!/usr/bin/env python3
import json
import datetime
import decimal
import sys
import io
import pathlib
import argparse
import time

import parsedatetime
from beancount.ingest import importer, extract
from beancount.query.query import run_query
from beancount.loader import load_string
from beancount.core import data, flags, amount


class Importer(importer.ImporterProtocol):

    def __init__(self):
        self.counter = 0

    def identify(self, file):
        if not file.name.endswith('.json'):
            return False
        return True

    def get_new_meta(self, file_name):
        self.counter += 1
        return data.new_metadata(file_name, self.counter)

    def get_postings_from_expense(self, expense):
        postings = []
        postings.append(data.Posting(
            f'Liabilities:Bois:{expense["who"]}',
            -amount.Amount(decimal.Decimal(expense['amount']), 'DOP'),
            None,
            None,
            None,
            None
        ))
        postings.append(data.Posting(
            f'Expenses:{expense["expense"].replace(" ", "")}',
            amount.Amount(decimal.Decimal(expense['amount']), 'DOP'),
            None,
            None,
            None,
            None
        ))
        return postings

    def get_postings_from_payment(self, payment):
        postings = []
        # TODO: Verify who is a boi
        postings.append(data.Posting(
            f'Liabilities:Bois:{payment["who"]}',
            amount.Amount(decimal.Decimal(payment['amount']), 'DOP'),
            None,
            None,
            None,
            None
        ))
        postings.append(data.Posting(
            'Assets:CoroPago',
            -amount.Amount(decimal.Decimal(payment['amount']), 'DOP'),
            None,
            None,
            None,
            None
        ))
        return postings

    def create_debt_transaction_from_expense(self, file, expense, bois):
        postings = []
        amnt = decimal.Decimal(expense['amount'])
        # TODO: Verify who is a boi
        postings.append(data.Posting(
            f'Liabilities:Bois:{expense["who"]}',
            amount.Amount(amnt, 'DOP'),
            None,
            None,
            None,
            None
        ))
        # TODO: Verify all_bois are bois
        all_bois = set(expense['split_between'] + [expense['who']])
        debt = round(amnt/len(all_bois), 2)
        for boi in all_bois:
            if boi == expense['who']:
                continue
            postings.append(data.Posting(
                f'Liabilities:Bois:{boi}',
                -amount.Amount(debt, 'DOP'),
                None,
                None,
                None,
                None
            ))
        postings.append(data.Posting(
            f'Assets:CoroPago',
            -amount.Amount(amnt - debt*(len(all_bois)-1), 'DOP'),
            None,
            None,
            None,
            None
        ))
        return data.Transaction(
            self.get_new_meta(file.name),
            datetime.date.fromisoformat(expense['when']),
            flags.FLAG_OKAY,
            None,
            f'Se le debe a {expense["who"]} uwu',
            {expense['who']},
            data.EMPTY_SET,
            postings
        )

    def extract(self, file, existing_entries=None):
        finanzas_dict = {}
        with open(file.name) as f:
            finanzas_dict = json.loads(f.read())
        entries = []
        for boi in finanzas_dict['bois']:
            entries.append(data.Open(
                self.get_new_meta(file.name),
                # TODO: Add date from first time the boi is shown in expenses
                # here I'm assuming first datetime is the earliest date
                datetime.date.fromisoformat(finanzas_dict['expenses'][0]['when']),
                f'Liabilities:Bois:{boi}',
                '',
                data.Booking.NONE
            ))

        entries.append(data.Open(
            self.get_new_meta(file.name),
            # here I'm assuming first datetime is the earliest date
            datetime.date.fromisoformat(finanzas_dict['expenses'][0]['when']),
            'Assets:CoroPago',
            '',
            data.Booking.NONE
        ))
        for expense in finanzas_dict['expenses']:
            # TODO: Verify who is a boi
            entries.append(data.Transaction(
                self.get_new_meta(file.name),
                datetime.date.fromisoformat(expense['when']),
                flags.FLAG_OKAY,
                None,
                '',
                {expense['who']},
                data.EMPTY_SET,
                self.get_postings_from_expense(expense)
            ))
            entries.append(
                self.create_debt_transaction_from_expense(
                    file,
                    expense,
                    finanzas_dict['bois']
                )
            )

        for payment in finanzas_dict['payments']:
            # TODO: Verify to is a boi
            entries.append(data.Transaction(
                self.get_new_meta(file.name),
                datetime.date.fromisoformat(payment['when']),
                flags.FLAG_OKAY,
                None,
                '',
                {payment['to']},
                data.EMPTY_SET,
                self.get_postings_from_payment(payment)
            ))
        return entries


CONFIG = [
    Importer()
]


def loads():
    file = io.StringIO()
    extract.extract(
        importer_config=CONFIG,
        files_or_directories=[str(pathlib.Path('./registro.json').absolute())],
        output=file
    )
    file.seek(0)
    return file.read()


def parse_datetime(relative_or_iso: str) -> str:
    cal = parsedatetime.Calendar()
    time_struct, parse_status = cal.parse(relative_or_iso)
    return f'{time_struct[0]}-{time_struct[1]}-{time_struct[2]}'


def get_args():
    parser = argparse.ArgumentParser(description='Lo paga malo.')

    # TODO: Add choices to be only bois names
    parser.add_argument('boi_name', help='Nombre del boi al que quieres ver quienes les deben.', type=str)
    parser.add_argument('-d', '--date', help='Relative or ISO date', type=str)
    parser.add_argument('--keepdb', help='', action=argparse.BooleanOptionalAction)

    args = parser.parse_args()
    return args


def main():
    if len(sys.argv) == 1:
        return
    args = get_args()
    a_quien_se_le_debe = args.boi_name
    # TODO: Check if the output doesnt have unbalanced transactions uwu
    file_content = loads()
    if args.keepdb:
        with open('db.beancount', 'w') as f:
            f.write(file_content)
    entries, errors, options = load_string(file_content)
    if not a_quien_se_le_debe:
        return
    select = f"select account, sum(position) where '{a_quien_se_le_debe}' in tags"
    if args.date:
        select += f" and date = DATE('{parse_datetime(args.date)}')"
    res_types, res_rows = run_query(
        entries,
        options,
        select,
        numberify=True
    )
    for row in res_rows:
        if not row[1]:
            continue
        if row[0] == 'Assets:CoroPago':
            continue
        if row[0].startswith('Expense'):
            print('En que gasto ->', end=' ')
        print(row[0].split(':')[-1], end=' ')
        if row[0].startswith('Liabilities'):
            print('le debe', end=' ')
            row[1] *= -1
        print(row[1])


if __name__ == '__main__':
    main()

