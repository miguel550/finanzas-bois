#!/usr/bin/env python3
import json
import datetime
import decimal
import sys
import io
import pathlib
import argparse

import parsedatetime
from beancount.ingest import importer, extract
from beancount.query.query import run_query
from beancount.loader import load_string
from beancount.core import data, flags, amount


# TODO: Fix when two bois owe each other money for example:
# Will owe 303.34 to Pedro and Pedro owe 123.33 to Will,
# It should say that Pedro doesn't owe to Will and Will only owes 180.01 to Pedro

class BoizRegisterImporter(importer.ImporterProtocol):

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

    def get_boi_account(self, boi_name):
        if not self.bois:
            raise Exception('Bois var is not defined')
        if boi_name not in self.bois:
            raise Exception(f'Boi {boi_name} doesnt exists')
        return f'Liabilities:Bois:{boi_name}'

    def get_coro_pago_account(self):
        return 'Assets:CoroPago'

    def get_postings_from_payment(self, payment):
        postings = []
        postings.append(data.Posting(
            self.get_boi_account(payment["who"]),
            amount.Amount(decimal.Decimal(payment['amount']), 'DOP'),
            None,
            None,
            None,
            None
        ))
        postings.append(data.Posting(
            self.get_coro_pago_account(),
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
        postings.append(data.Posting(
            self.get_boi_account(expense["who"]),
            amount.Amount(amnt, 'DOP'),
            None,
            None,
            None,
            None
        ))
        all_bois = set(expense['split_between'] + [expense['who']])
        debt = round(amnt/len(all_bois), 2)
        for boi in all_bois:
            if boi == expense['who']:
                continue
            postings.append(data.Posting(
                self.get_boi_account(boi),
                -amount.Amount(debt, 'DOP'),
                None,
                None,
                None,
                None
            ))
        postings.append(data.Posting(
            self.get_coro_pago_account(),
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
        self.bois = finanzas_dict['bois']
        for boi in finanzas_dict['bois']:
            entries.append(data.Open(
                self.get_new_meta(file.name),
                # TODO: Add date from first time the boi is shown in expenses
                # here I'm assuming first datetime is the earliest date
                datetime.date.fromisoformat(finanzas_dict['expenses'][0]['when']),
                self.get_boi_account(boi),
                '',
                data.Booking.NONE
            ))

        entries.append(data.Open(
            self.get_new_meta(file.name),
            # here I'm assuming first datetime is the earliest date
            datetime.date.fromisoformat(finanzas_dict['expenses'][0]['when']),
            self.get_coro_pago_account(),
            '',
            data.Booking.NONE
        ))
        for expense in finanzas_dict['expenses']:
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
    BoizRegisterImporter()
]


REGISTRO_PATH = str(pathlib.Path('./registro.json').absolute())


def loads():
    file = io.StringIO()
    extract.extract(
        importer_config=CONFIG,
        files_or_directories=[REGISTRO_PATH],
        output=file
    )
    file.seek(0)
    return file.read()


def parse_datetime(relative_or_iso: str) -> str:
    cal = parsedatetime.Calendar()
    time_struct, parse_status = cal.parse(relative_or_iso)
    return f'{time_struct[0]}-{time_struct[1]}-{time_struct[2]}'


def get_args(bois: list):
    parser = argparse.ArgumentParser(description='Lo paga malo.')

    parser.add_argument('boi_name', choices=bois, help='Nombre del boi al que quieres ver quienes les deben.', type=str)
    parser.add_argument('-d', '--date', help='Relative or ISO date', type=str)
    parser.add_argument('--keepdb', help='', action=argparse.BooleanOptionalAction)

    args = parser.parse_args()
    return args


def get_bois_names():
    with open(REGISTRO_PATH, 'r') as f:
        j = json.load(f)
        return j['bois']


# TODO: It should say why a boi owe that amount to the boi
# Possible fix: modify narration to show for example 1/6th of Expense name
def main():
    if len(sys.argv) == 1:
        return
    args = get_args(get_bois_names())
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

