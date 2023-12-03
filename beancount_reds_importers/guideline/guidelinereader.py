"""csv importer module for beancount to be used along with investment/banking/other importer modules in
beancount_reds_importers."""

import datetime
import re
import regex
import traceback
from beancount.ingest import importer
from beancount.core.number import D
from beancount.core.number import Decimal
import petl as etl
from beancount_reds_importers.libreader import reader
from .extract_statement import extractTextStatement
from .regex_formatter import *

from enum import Enum

AggrOps = Enum('AggrOps', ['PAYROLL', 'FEE', 'EXCHANGE', 'DIVIDEND'])
Postings = Enum('Postings', ['BUY', 'SELL', 'REINVEST'])

class Importer(reader.Reader, importer.ImporterProtocol):
    FILE_EXT = 'pdf'

    def prepare_raw_columns(self, rdr):
        if '' in rdr.fieldnames():
            rdr = rdr.cutout('')  # clean up last column

        def cleanup_date(d):
            """'11/16/2018 as of 11/15/2018' --> '11/16/2018'"""
            return d.split(' ', 1)[0]

        rdr = rdr.convert('Date', cleanup_date)
        rdr = rdr.addfield('tradeDate', lambda x: x['Date'])
        rdr = rdr.addfield('total', lambda x: x['Amount'])
        return rdr


    def initialize_reader(self, file):
        if getattr(self, 'file', None) != file:
            self.file = file
            self.reader_ready = True # re.match(self.header_identifier, file.name)
            if self.reader_ready:
                # TODO: move out elsewhere?
                # self.currency = self.ofx_account.statement.currency.upper()
                self.currency = self.config.get('currency', 'USD')
                self.date_format = '%m/%d/%Y'  # TODO: move into class variable, into reader.Reader
                self.file_read_done = False
            # else:
            #     print("header_identifier failed---------------:")
            #     print(self.header_identifier, file.head())

    def file_date(self, file):
        "Get the maximum date from the file."
        self.read_file(file)
        return max(ot.date for ot in self.get_transactions()).date()


    def prepare_processed_columns(self, rdr):
        return rdr

    def convert_columns(self, rdr):
        # convert data in transaction types column
        if 'type' in rdr.header():
            rdr = rdr.convert('type', self.transaction_type_map)

        # fixup decimals
        decimals = ['units']
        for i in decimals:
            if i in rdr.header():
                rdr = rdr.convert(i, D)

        # fixup currencies
        def remove_non_numeric(x):
            return re.sub("[^0-9\.-]", "", str(x).strip())  # noqa: W605
        currencies = ['unit_price', 'fees', 'total', 'amount', 'balance']
        for i in currencies:
            if i in rdr.header():
                rdr = rdr.convert(i, remove_non_numeric)
                rdr = rdr.convert(i, D)

        # fixup dates
        def convert_date(d):
            return datetime.datetime.strptime(d, self.date_format)
        dates = ['date', 'tradeDate', 'settleDate']
        for i in dates:
            if i in rdr.header():
                rdr = rdr.convert(i, convert_date)

        return rdr


    def read_file(self, file):
        if not self.file_read_done:
            text = extractTextStatement(file.name)
            # Get statement year
            statement_year = regex.search(statement_year_regex, text)
            statement_year = statement_year.group('year').strip()
            num_lines = len(text.splitlines())
            aggregate_ops, all_postings = self._getOperations(text)
            csv = [['Action', 'Date', 'Description', 'Symbol', 'Quantity', 'Price', 'Amount', "Fees & Comm"]]
            for iaggr, aggr in enumerate(aggregate_ops):
                min_line = aggr[0]
                max_line = aggregate_ops[iaggr+1][0] if iaggr+1<len(aggregate_ops) else num_lines
                date = aggr[2]['date'] + '/' + statement_year
                postings = [post for post in all_postings if post[0] in range(min_line, max_line)]
                for post in postings:
                    amount = post[2]['amount']
                    price = amount/post[2]['qty']
                    if aggr[1] != AggrOps.FEE:
                        op = [post[1], date, '', post[2]['ticker'], str(post[2]['qty']), str(price), str(amount), '']
                    else:
                        op = [post[1], date, '', post[2]['ticker'], str(post[2]['qty']), str(price), '', str(amount)]
                    csv.append(op)
            rdr = etl.head(csv, len(csv)-1)
            # rdr = rdr.skip(getattr(self, 'skip_head_rows', 0))                 # chop unwanted header rows
            # rdr = rdr.head(len(rdr) - getattr(self, 'skip_tail_rows', 0) - 1)  # chop unwanted footer rows

            if hasattr(self, 'skip_comments'):
                rdr = rdr.skipcomments(self.skip_comments)
            rdr = rdr.rowslice(getattr(self, 'skip_data_rows', 0), None)
            rdr = self.prepare_raw_columns(rdr)
            rdr = rdr.rename(self.header_map)
            rdr = self.convert_columns(rdr)
            rdr = self.prepare_processed_columns(rdr)
            self.rdr = rdr
            self.ifile = file
            self.file_read_done = True
                
    ## Utils ##
    ###########

    def _getOperations(self, parsed_statement):
        # Clean-up statement string
        statement = regex.sub(
            one_character_line_regex,
            'FLAG_DELETE_THIS_LINE',
            parsed_statement,
            flags=regex.M,
        )  # flag lines with one character or less
        statement = '\n'.join(
            [
                s
                for s in statement.splitlines()
                if 'FLAG_DELETE_THIS_LINE' not in s
            ]
        )  # keep only non-flaged lines

        # Get statement year
        statement_year = regex.search(statement_year_regex, statement)
        statement_year = statement_year.group('year').strip()

        account = self._searchAccounts(statement)

        operations = []

        if account in self.config['account_number']:


            # create total for inconsistency check
            total = Decimal(0.0)

            # clean account to keep only operations
            clean_statement = self._clean_statement(statement)
            # clean_file = path.join(*['/home', 'stefano', 'Downloads' ,'Guideline-simple.txt'])

            # with open(clean_file, 'w') as f:
            #     f.write(clean_statement)

            # get all line numbers
            end='.*\n'
            line=[]
            for m in regex.finditer(end, clean_statement):
                line.append(m.end())

            # the statement contains some aggregate lines that specify an
            # amount for a payroll, fee or exchange transaction.
            # each of these aggregate lines is followed by buy or sell operations
            # for each single ticker, which don't have a date, so we need
            # to get the date from the aggregate operation.
            # A payroll transaction is generally followed by a number of buy
            # operations, and it represents money coming in from a paycheck.
            # A fee operation involves mostly sell operations, but sometimes
            # buy operations, too.
            # An exchange operation is generally followed by a pair of sell/buy
            # operations.
            # After these operations we have dividend reinvestments, which have
            # an aggregate reinvest operation with a date, and the actual
            # transaction with a ticket

            # get all the aggregate ops, along with their line numbers
            
            
            # search all payroll operations
            aggregate_operations = []
            for operation in regex.finditer(payroll_transaction_regex, clean_statement, flags=regex.MULTILINE):
                aggregate_operations.append([next(i for i in range(len(line)) if line[i]>operation.start(1)), AggrOps.PAYROLL, operation.groupdict()])
            for operation in regex.finditer(fee_transaction_regex, clean_statement, flags=regex.MULTILINE):
                aggregate_operations.append([next(i for i in range(len(line)) if line[i]>operation.start(1)), AggrOps.FEE, operation.groupdict()])
            for operation in regex.finditer(exchange_transaction_regex, clean_statement, flags=regex.MULTILINE):
                aggregate_operations.append([next(i for i in range(len(line)) if line[i]>operation.start(1)), AggrOps.EXCHANGE, operation.groupdict()])
            for operation in regex.finditer(dividend_transaction_regex, clean_statement, flags=regex.MULTILINE):
                aggregate_operations.append([next(i for i in range(len(line)) if line[i]>operation.start(1)), AggrOps.DIVIDEND, operation.groupdict()])
            aggregate_operations = sorted(aggregate_operations, key=lambda x: x[0])

            def isfloat(string):
                try:
                    float(string)
                    return True
                except:
                    return False

            for op in aggregate_operations:
                op[2] = {k: v.replace(',','') for k,v in op[2].items()}
                op[2] = {k: float(v) if isfloat(v) else v for k,v in op[2].items()}
            
            
            postings = []
            for post in regex.finditer(buy_transaction_regex, clean_statement, flags=regex.MULTILINE):
                postings.append([next(i for i in range(len(line)) if line[i]>post.start(1)), 'Buy', post.groupdict()])
            for post in regex.finditer(sell_transaction_regex, clean_statement, flags=regex.MULTILINE):
                postings.append([next(i for i in range(len(line)) if line[i]>post.start(1)), 'Sell', post.groupdict()])
            for post in regex.finditer(reinvest_transaction_regex, clean_statement, flags=regex.MULTILINE):
                postings.append([next(i for i in range(len(line)) if line[i]>post.start(1)), 'Reinvest Dividend', post.groupdict()])
            postings = sorted(postings, key=lambda x: x[0])

            for op in postings:
                op[2] = {k: v.replace(',','') for k,v in op[2].items()}
                op[2] = {k: float(v) if isfloat(v) else v for k,v in op[2].items()}

        return aggregate_operations, postings

    
    def _searchAccounts(self, statement):
        
        accounts = regex.findall(account_regex, statement, flags=regex.M)
        if len(accounts) > 1:
            raise
        return accounts[0]

    
    def _clean_statement(self, statement):
        # split the text by the 'new_balance_regex' line
        cleaned = regex.split(transaction_begin_regex, statement, flags=regex.M)
        # keep the last parts (i.e. everything that's after the 'transaction_begin_regex' line)
        cleaned = '\n'.join(cleaned[1:])
        # flag lines with specific words
        words_to_remove = [
            'ACTION',
            'guideline.com',
            'Investment Income',
        ]
        words_to_remove_regex = (
            r'^.*\b(' + '|'.join(words_to_remove) + r')\b.*$'
        )
        # flag lines longer than 70
        cleaned = regex.sub(
            longer_than_70_regex,
            'FLAG_DELETE_THIS_LINE',
            cleaned,
            flags=regex.M,
        )
        # flag lines with words to remove
        cleaned = regex.sub(
            words_to_remove_regex,
            'FLAG_DELETE_THIS_LINE',
            cleaned,
            flags=regex.M,
        )
        # remove trailing spaces
        cleaned = regex.sub(
            trailing_spaces_and_tabs_regex, '', cleaned, flags=regex.M
        )
        # flag empty lines
        cleaned = regex.sub(
            empty_line_regex, 'FLAG_DELETE_THIS_LINE', cleaned, flags=regex.M
        )
        # flag lines with less than 2 characters
        cleaned = regex.sub(
            smaller_than_2_regex,
            'FLAG_DELETE_THIS_LINE',
            cleaned,
            flags=regex.M,
        )
        # keep only non-flaged lines
        cleaned = '\n'.join(
            [
                s
                for s in cleaned.splitlines()
                if 'FLAG_DELETE_THIS_LINE' not in s
            ]
        )
        return cleaned


    def get_transactions(self):
        for ot in self.rdr.namedtuples():
            if self.skip_transaction(ot):
                continue
            yield ot

    def get_balance_positions(self):
        return []

    def get_available_cash(self, settlement_fund_balance=0):
        return None

    # TOOD: custom, overridable
    def skip_transaction(self, row):
        return getattr(row, 'type', 'NO_TYPE') in self.skip_transaction_types

    def get_max_transaction_date(self):
        try:
            # date = self.ofx_account.statement.end_date.date() # this is the date of ofx download
            # we find the last transaction's date. If we use the ofx download date (if our source is ofx), we
            # could end up with a gap in time between the last transaction's date and balance assertion.
            # Pending (but not yet downloaded) transactions in this gap will get downloaded the next time we
            # do a download in the future, and cause the balance assertions to be invalid.

            # TODO: clean this up. this probably suffices:
            # return max(ot.date for ot in self.get_transactions()).date()
            date = max(ot.tradeDate if hasattr(ot, 'tradeDate') else ot.date
                       for ot in self.get_transactions()).date()
        except Exception as err:
            print("ERROR: no end_date. SKIPPING input.")
            traceback.print_tb(err.__traceback__)
            return False

        return date
