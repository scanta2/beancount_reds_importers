from datetime import datetime
import regex
import math

from beancount.core import data, flags
from beancount.core.amount import Amount
from beancount.core.number import Decimal
from beancount.core.position import CostSpec
from beancount.ingest import importer

from .extract_statement import extractTextStatement
from .regex_formatter import *
from enum import Enum

AggrOps = Enum('AggrOps', ['PAYROLL', 'FEE', 'EXCHANGE', 'DIVIDEND'])
Postings = Enum('Postings', ['BUY', 'SELL', 'REINVEST'])

class GuidelineImporter_PDF(importer.ImporterProtocol):
    """Beancount Importer for Guideline retirement account PDF statement exports.

    Attributes:
        iban (str): International Bank Account Number of the account you want to extract operations. Note that only the account number is necessary
        account (str): Account name in beancount format (e.g. 'Assets:FR:CdE:CompteCourant')
        expenseCat (str, optional): Expense category in beancount format (e.g. 'Expenses:FIXME'). Defaults to '', no expense posting added to the operation.
        creditCat (str, optional): Income category in beancount format (e.g. 'Income:FIXME'). Defaults to '', no income posting added to the operation.
        showOperationTypes (bool, optional): Show or not operation type (CARDDEBIT, WIRETRANSFER, CHECK ...) in header. Defaults to False.
    """

    def __init__(
        self,
        account_number: str,
        account: str,
        expenseCat: str = '',
        creditCat: str = '',
        showOperationTypes: bool = False,
    ):
        self.account_number = account_number
        self.account = account
        self.expenseCat = expenseCat
        self.creditCat = creditCat
        self.showOperationTypes = showOperationTypes

    ## API Methods ##
    #################

    def name(self):
        return 'Guideline {}'.format(self.__class__.__name__)

    def file_account(self, _):
        return self.account

    def file_date(self, file_):
        if not self.identify(file_):
            return None
        text = extractTextStatement(file_.name)
        return self._searchEmissionDate(text)

    def file_name(self, _):
        return 'CaisseEpargne_Statement.pdf'

    def identify(self, file_) -> bool:
        b = False
        try:
            text = extractTextStatement(file_.name)
            if 'Administrator: Guideline' in text:
                b = True
        except:
            pass
        return b

    def extract(self, file_, existing_entries=None):
        entries = []

        if not self.identify(file_):
            return []

        if type(file_) == str:
            text = extractTextStatement(file_)
        else:
            text = extractTextStatement(file_.name)

        aggregate_ops, all_postings = self._getOperations(text)
        max_line_end = max(aggregate_ops[-1][0], all_postings[-1][0])+1

        for index, op in enumerate(aggregate_ops):

            line_begin = op[0]
            line_end = aggregate_ops[index+1][0] if index < len(aggregate_ops)-1 else max_line_end

            file_postings = [post for post in all_postings if line_begin < post[0] < line_end]

            if any(p[1] == Postings.SELL for p in file_postings):
                continue

            total_amount = 0
            if op[1] == AggrOps.PAYROLL or op[1] == AggrOps.DIVIDEND:
                total_amount = op[2]['amount']
            elif op[1] == AggrOps.FEE:
                total_amount = -op[2]['amount']
            running_amount = 0

            currency = 'USD'

            for line_post in file_postings:
                if type(file_) == str:
                    meta = data.new_metadata(file_, line_post[0])
                else:
                    meta = data.new_metadata(file_.name, line_post[0])

                postings = []

                if op[1] == AggrOps.PAYROLL:
                    amount = line_post[2]['amount']
                    running_amount += amount
                    amount_dec = Decimal(amount)
                    quantity = Decimal(line_post[2]['qty'])
                    postings.append(
                        data.Posting(
                            self.account+':Cash',
                            Amount(-amount_dec, currency),
                            None,
                            None,
                            None,
                            None,
                        )
                    )
                    postings.append(
                        data.Posting(
                            self.account+':'+line_post[2]['ticker'],
                            Amount(quantity, line_post[2]['ticker']),
                            CostSpec(None,amount_dec,currency,None,None,None),
                            None,
                            None,
                            None,
                        )
                    )
                    narration = 'Buy ' + str(line_post[2]['qty']) + ' '+line_post[2]['ticker']
                    entries.append(
                        data.Transaction(
                            meta,
                            datetime.strptime(op[2]['date']+'/2022', '%m/%d/%Y').date(),
                            flags.FLAG_OKAY,
                            '',
                            narration,
                            data.EMPTY_SET,
                            data.EMPTY_SET,
                            postings,
                        )
                    )
                elif op[1] == AggrOps.FEE:
                    pass
                elif op[1] == AggrOps.EXCHANGE:
                    pass
                elif op[1] == AggrOps.DIVIDEND:
                    pass
            if op[1] == AggrOps.PAYROLL and not math.isclose(running_amount, total_amount, abs_tol=0.01):
                raise

        return entries

    ## Utils ##
    ###########

    def getOperations(self, parsed_statement):
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

        if account in self.account_number:


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
                postings.append([next(i for i in range(len(line)) if line[i]>post.start(1)), Postings.BUY, post.groupdict()])
            for post in regex.finditer(sell_transaction_regex, clean_statement, flags=regex.MULTILINE):
                postings.append([next(i for i in range(len(line)) if line[i]>post.start(1)), Postings.SELL, post.groupdict()])
            for post in regex.finditer(reinvest_transaction_regex, clean_statement, flags=regex.MULTILINE):
                postings.append([next(i for i in range(len(line)) if line[i]>post.start(1)), Postings.REINVEST, post.groupdict()])
            postings = sorted(postings, key=lambda x: x[0])

            for op in postings:
                op[2] = {k: v.replace(',','') for k,v in op[2].items()}
                op[2] = {k: float(v) if isfloat(v) else v for k,v in op[2].items()}
            
            
            
                # debitLine = account[debit_op.start() : debit_op.end()]
                # debitLine = debitLine.split('\n')[0]
                # debitLineList = debitLine.split(' ')[1:-1]
                # debitLineCleanList = []
                # for w in debitLineList:
                #     if w == 'FACT':
                #         break
                #     if len(w) > 0:
                #         debitLineCleanList.append(w)
                # debitLine = ''
                # for i, w in enumerate(debitLineCleanList):
                #     debitLine += ('' if i == 0 else ' ') + w
                # # extract regex groups
                # op_date = debit_op.group('op_dte').strip()
                # op_label = debit_op.group('op_lbl').strip()
                # op_label_extra = debit_op.group('op_lbl_extra').strip()
                # op_amount = debit_op.group('op_amt').strip()
                # # convert amount to regular Decimal
                # op_amount = op_amount.replace(',', '.')
                # op_amount = op_amount.replace(' ', '')
                # op_amount = Decimal(op_amount)
                # # update total
                # total -= op_amount
                # # print('debit {0}'.format(op_amount))
                # operations.append(
                #     self._create_operation_entry(
                #         op_date,
                #         statement_year,
                #         full,
                #         op_label,
                #         debitLine,
                #         op_amount,
                #         True,
                #     )
                # )

            # # search all credit operations
            # credit_ops = regex.finditer(
            #     credit_regex, account, flags=regex.M
            # )
            # for credit_op in credit_ops:
            #     # extract regex groups
            #     op_date = credit_op.group('op_dte').strip()
            #     op_label = credit_op.group('op_lbl').strip()
            #     op_label_extra = credit_op.group('op_lbl_extra').strip()
            #     op_amount = credit_op.group('op_amt').strip()

            #     creditLine = account[credit_op.start() : credit_op.end()]
            #     creditLine = creditLine.split('\n')[0]
            #     creditLine = creditLine[len(op_amount) + len(op_date) :]

            #     # convert amount to regular Decimal
            #     op_amount = op_amount.replace(',', '.')
            #     op_amount = op_amount.replace(' ', '')
            #     op_amount = Decimal(op_amount)
            #     # update total
            #     total += op_amount
            #     # print('credit {0}'.format(op_amount))

            #     operations.append(
            #         self._create_operation_entry(
            #             op_date,
            #             statement_year,
            #             full,
            #             op_label,
            #             creditLine,
            #             op_amount,
            #             False,
            #         )
            #     )

        return aggregate_operations, postings

    def _create_operation_entry(
        self,
        op_date,
        statement_emission_date,
        account_number,
        op_label,
        op_label_extra,
        op_amount,
        debit,
    ):
        # search the operation type according to its label
        op_type = self._search_operation_type(op_label)

        op = [
            self._set_operation_year(op_date, statement_emission_date),
            account_number,
            op_type,
            op_label.strip(),
            # op_label_extra.strip().replace('\n','\\'),
            op_label_extra.strip(),
            # the star '*' operator is like spread '...' in JS
            *(['', '%.2f' % op_amount] if debit else ['%.2f' % op_amount, '']),
        ]
        return op

    def _search_operation_type(self, op_label):
        op_label = op_label.upper()
        # bank fees, international fees, subscription fee to bouquet, etc.
        if (op_label.startswith('*')) or (op_label.startswith('INTERETS')):
            opType = 'BANK'
        # cash deposits on the account
        elif op_label.startswith('VERSEMENT'):
            opType = 'DEPOSIT'
        # incoming / outcoming wire transfers: salary, p2p, etc.
        elif (op_label.startswith('VIREMENT')) or (op_label.startswith('VIR')):
            opType = 'WIRETRANSFER'
        # check deposits / payments
        elif (
            (op_label.startswith('CHEQUE'))
            or (op_label.startswith('REMISE CHEQUES'))
            or (op_label.startswith('REMISE CHQ'))
        ):
            opType = 'CHECK'
        # payments made via debit card
        elif op_label.startswith('CB'):
            opType = 'CARDDEBIT'
        # withdrawals
        elif (op_label.startswith('RETRAIT')) or (
            op_label.startswith('RET DAB')
        ):
            opType = 'WITHDRAWAL'
        # direct debits
        elif op_label.startswith('PRLV'):
            opType = 'DIRECTDEBIT'
        else:
            opType = 'OTHER'

        return opType

    def _set_operation_year(self, emission, statement_emission_date):
        # fake a leap year
        emission = datetime.strptime(emission + '00', '%d/%m%y')
        if emission.month <= statement_emission_date.month:
            emission = emission.replace(year=statement_emission_date.year)
        else:
            emission = emission.replace(year=statement_emission_date.year - 1)
        return datetime.strftime(emission, '%d/%m/%Y')

    def _search_account_owner(self, regex_to_use, statement):
        # search for owner to identify multiple accounts
        account_owner = regex.search(regex_to_use, statement, flags=regex.M)
        if not account_owner:
            raise ValueError('No account owner was found.')
        # extract and strip
        account_owner = account_owner.group('owner').strip()
        return account_owner

    def _searchEmissionDate(self, statement):
        emission_date = regex.search(emission_date_regex, statement)
        # extract and strip
        emission_date = emission_date.group('date').strip()
        # parse date
        emission_date = datetime.strptime(emission_date, '%d/%m/%Y')
        return emission_date.date()

    def _searchAccounts(self, statement):
        
        accounts = regex.findall(account_regex, statement, flags=regex.M)
        if len(accounts) > 1:
            raise
        return accounts[0]
