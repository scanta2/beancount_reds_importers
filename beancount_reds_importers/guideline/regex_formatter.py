# # - will match owner
# # prior to march 2019
# owner_regex_v1 = r'Identifiant client\s+(?P<owner>\D*)'
# # after march 2019
# owner_regex_v2 = r'^(?P<title>MR|MME|MLLE)\s+(?P<owner>\D*?)$'

# # - will match dates
# emission_date_regex = r'\b(?P<date>[\d/]{10})\b'

# # - will match debits
# # Ex 1.
# # 18/10 CB CENTRE LECLERC  FACT 161014      13,40
# # Ex 2.
# # 27/05 PRLV FREE MOBILE      3,99
# # -Réf. donneur d'ordre :
# # fmpmt-XXXXXXXX
# # -Réf. du mandat : FM-XXXXXXXX-X
# # [\S\s].*?
# debit_regex = (
#     r'^'
#     # date: dd/dd
#     r'(?P<op_dte>\d\d\/\d\d)'
#     # label: any single character (.), between 0 and unlimited (*), lazy (?)
#     r'(?P<op_lbl>.*?)'
#     # any whitespace and non-whitespace character (i.e. any character) ([\S\s]), any character (.) between 0 and unlimited (+), lazy
#     r'\s.*?'
#     # amount: alternative between ddd ddd,dd and ddd,dd, until the end of line ($)
#     #    the positive lookebehind assures that there is at least one white space before any amount
#     #    the positive lookbehind handles the following case where amount to match is 4,45 and not 14,40:
#     #    19/10 INTERETS TAEG 14,40
#     #    VALEUR AU 18/10     4,45
#     r'(?P<op_amt>(?<=\s)\d{1,3}\s{1}\d{1,3}\,\d{2}|\d{1,3}\,\d{2}(?!([\S\s].*?((?<=(?=(^(?!(?1))\s.*(?1))))\s.*(?3)))))$'
#     # any whitespace character (\s), between 0 and unlimited (*), greedy
#     r'\s*'
#     # extra label: 'single line mode' until the positive lookehead is satisfied
#     #    positive lookahead --> alternative between:
#     #       -line starting with first named subpatern (date)
#     #       -line starting with third named subpatern (amount)
#     #       -EOL
#     #    we use [\s\S]*? to do like the single line mode
#     #    basically it's going to match any non-whitespace OR whitespace character. That is, any character, including linebreaks.
#     #    we could have used (?s) to activate the real line mode...
#     #    ...but Python doesn't support mode-modified groups (meaning that it will change the mode for the whole regex)
#     r'(?P<op_lbl_extra>[\S\s]*?(?=^(?1)|^(?3)|\Z))'
# )

# # - will match credits
# # Ex 1.
# # 150,0008/11 VIREMENT PAR INTERNET
# # Ex 2.
# # 11,8011/02VIR SEPA LA MUTUELLE DES ETUDIA
# # XXXXX/XX/XX-XXXX/XXXXXXXXX
# # -Réf. donneur d'ordre :
# # XXXXX/XX/XX-XXXX/XXXXXXXXX
# credit_regex = (
#     r'^'
#     ## amount: alternative between ddd ddd,dd and ddd,dd
#     r'(?P<op_amt>\d{1,3}\s{1}\d{1,3}\,\d{2}|\d{1,3}\,\d{2})'
#     ## date: dd/dd
#     r'(?P<op_dte>\d\d\/\d\d)'
#     r'(?P<op_lbl>.*)$'
#     ## any whitespace character (\s), between 0 and unlimited (*), greedy
#     r'\s*'
#     ## extra label: 'single line mode' until the positive lookehead is satisfied
#     #    positive lookahead --> alternative between:
#     #      -line starting with first subpatern (amount)
#     #      -line starting with second subpatern (date)
#     #      -EOL
#     #    we use [\s\S]*? to do like the single line mode
#     #    basically it's going to match any non-whitespace OR whitespace character. That is, any character, including linebreaks.
#     #    we could have used (?s) to activate the real line mode...
#     #    ...but Python doesn't support mode-modified groups (meaning that it will change the mode for the whole regex)
#     r'(?P<op_lbl_extra>[\S\s]*?(?=^(?1)|^(?2)|\Z))'
# )

# # - will match new account balances
# #   NOUVEAU SOLDE CREDITEUR AU 15/11/14 (en francs : 1 026,44) 156,48
# new_balance_regex = r'NOUVEAU SOLDE CREDITEUR AU (?P<bal_dte>\d\d\/\d\d\/\d\d)\s+\(en francs : (?P<bal_amt_fr>[\d, ]+)\)\s+(?P<bal_amt>[\d, ]+?)$'
account_regex = r'^(?P<account>[A-Z0-9]+\-[A-Z0-9]+)$'
statement_year_regex = (
    r'^Quarterly Statement Q\d '
    r'(?P<year>\d+)'
)

transaction_begin_regex = r'DATE ACTION SOURCE PRE-TAX ROTH EMPLOYER TOTAL'

r'\b(?P<date>[\d/]{10})\b'
one_character_line_regex = r'^( +|.|\n)$'
longer_than_70_regex = r'^(.{70,})$'
smaller_than_2_regex = r'^.{,2}$'
empty_line_regex = r'^(\s*)$'
trailing_spaces_and_tabs_regex = r'[ \t]+$'
# r'(?P<op_dte>\d\d\/\d\d)'

payroll_transaction_regex = (
    r'^(?P<date>\d\d\/\d\d) Buy Payroll'
    r'(?:.*?)'
    r'\$(?P<amount>[\d.,]+)$'
)
fee_transaction_regex =  (
    r'^(?P<date>\d\d\/\d\d) Sell Account'
    r'(?:.*?)'
    r'\$(?P<amount>[\d.,]+)$'
)
exchange_transaction_regex =  (
    r'^(?P<date>\d\d\/\d\d) Exchange Rebalance'
    r'(?:.*?)'
    r'\$(?P<amount>[\d.,]+)$'
)
buy_transaction_regex = (
    r'^Buy '
    r'(?P<ticker>[A-Z]+)'
    r'\s(?P<qty>\d+.\d+)'
    r'(?:.*?)'
    r'\$(?P<amount>[\d.,]+)$'
)
sell_transaction_regex = (
    r'^Sell '
    r'(?P<ticker>[A-Z]+)'
    r'\s(?P<qty>\d+.\d+)'
    r'(?:.*?)'
    r'\$(?P<amount>[\d.,]+)$'
)
dividend_transaction_regex =  (
    r'^(?P<date>\d\d\/\d\d) Reinvest Dividend'
    r'(?:.*?)'
    r'\$(?P<amount>[\d.,]+)$'
)
reinvest_transaction_regex = (
    r'^Reinvest '
    r'(?P<ticker>[A-Z]+)'
    r'\s(?P<qty>\d+.\d+)'
    r'(?:.*?)'
    r'\$(?P<amount>[\d.,]+)$'
)

# from pdfminer.high_level import extract_text
# from pdfminer.layout import LAParams
# from os import path
# import regex

# word_margin = 1.0
# char_margin = 120.0
# line_margin = 0.3
# boxes_flow = 0.5  # LAParams

# # If any LAParams group arguments were passed, create an LAParams object and
# # populate with given args. Otherwise, set it to None.

# laparams = LAParams()
# for param in (
#     "word_margin",
#     "char_margin",
#     "line_margin",
#     "boxes_flow",
# ):
#     paramv = locals().get(param, None)
#     if paramv is not None:
#         setattr(laparams, param, paramv)
# pdf_file = path.join(*['/home', 'stefano', 'Downloads' ,'Guideline-statement-2022-10-01-to-2022-12-31.txt'])
# with open(pdf_file) as f:
#     text = f.read().splitlines()
# # text = extract_text(pdf_file=pdf_file, laparams=laparams)
# for line in text:
#     result = regex.search(account_regex, line)
#     if result:
#         print(result.groups())
