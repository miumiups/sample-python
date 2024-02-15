# from supabase import create_client, Client
from datetime import datetime
from PyPDF2 import PdfReader
import pandas as pd
import sqlite3
import uuid
import re

KCC_PW = "dummy_password"
K_BANK_PW = "dummy_password"

conn = sqlite3.connect('exp_reco.db')  # Connect to a SQLite database (or create it if it doesn't exist)
cursor = conn.cursor()  # Create a cursor object


def extract_pdf(in_path, pw, out_path):
    """
    :param in_path: File path of a statement in pdf
    :param pw: Password to decrypt the pdf
    :param out_path: File path of an output .txt file
    :return: out_path
    """

    reader = PdfReader(in_path)  # Create a PDF object

    if reader.is_encrypted:
        reader.decrypt(pw)

    text = ""
    for page in reader.pages:
        text += page.extract_text()

    output_file = out_path  # Specify the file path where you want to save the extracted text

    # Save the extracted text to the file
    with open(output_file, "w", encoding="utf-8") as text_file:
        text_file.write(text)

    print(f"Extracted text saved to {out_path}")
    return out_path


def read_kcc(file_path):
    """
    :param file_path: A file path to a .txt file containing KCC transactions
    :return: A list of transactions in tuples
    """
    kcc_date_format = "%d/%m/%y"

    lines = []
    with open(file_path, 'r', encoding='utf-8') as file:
        for line in file:
            txn_pattern = r"(\d{2}/\d{2}/\d{2}) (\d{2}/\d{2}/\d{2}) (.+?) ([\d,]+\.\d{2})"
            if re.match(txn_pattern, line):
                lines.append(line)
            elif re.match(r"Amount \(Baht\)" + txn_pattern, line):
                lines.append(line.replace("Amount (Baht)", ""))
            elif "SUBTOTAL FOR  4552" in line:
                break
            else:
                continue
                # print(f"Not parsed: {line}")
    print(f'No. of transactions: {len(lines)}')

    expenses = []
    for txn in lines:
        matched = re.search(txn_pattern, txn)
        if matched:
            txn_date, posting_date, description, amount = matched.groups()

            # Create a tuple with extracted data and append it to the 'expenses' list
            exp_data = (
                None,
                None,
                datetime.now(),
                datetime.strptime(txn_date, kcc_date_format).date().isoformat(),
                datetime.strptime(posting_date, kcc_date_format).date().isoformat(),
                description,
                float(amount.replace(',', ''))
            )
            expenses.append(exp_data)
    return expenses


def insert_db_kcc(data):
    create_table_query = """
    CREATE TABLE IF NOT EXISTS "KCC" (
        id TEXT PRIMARY KEY,
        matched TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
        transaction_date DATE NOT NULL,
        posting_date DATE NOT NULL,
        description TEXT,
        amount NUMERIC NOT NULL
    )
    """
    # Execute the SQL command to create the table
    cursor.execute(create_table_query)

    # SQL command to insert data into the "KCC" table
    insert_data_query = """
    INSERT INTO "KCC" (id, matched, created_at, transaction_date, posting_date, description, amount)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    """

    # Execute the SQL command for each row
    for row in data:
        row = (f"kc{str(uuid.uuid4().int)}", *row[1:])
        cursor.execute(insert_data_query, row)

    # Commit the changes
    conn.commit()


def update_kcc(in_path, pw, out_path):
    out_path = extract_pdf(in_path, pw, out_path)
    kcc_data = read_kcc(out_path)
    insert_db_kcc(kcc_data)


def clean_kbank(file_path):
    formatted_lines = []

    # Read the content from the .txt file
    with open(file_path, 'r', encoding='utf-8') as file:
        lines = file.readlines()

        # Process and format the lines to remove line breaks within transactions
        current_transaction = None
        for line in lines:
            if current_transaction is None:
                if re.match(r'\d{2}-\d{2}-\d{2} \d{2}:\d{2}', line):
                    current_transaction = line
            else:
                if re.match(r'\d{2}-\d{2}-\d{2} \d{2}:\d{2}', line):
                    # If a line starts with the date-time pattern, it's the start of a new transaction
                    if current_transaction:
                        formatted_lines.append(current_transaction.strip())
                    current_transaction = line.strip()
                else:
                    # Otherwise, it's part of the current transaction
                    current_transaction += ' ' + line.strip()
        # Append the last transaction
        if current_transaction:
            formatted_lines.append(current_transaction.strip())

    print(f'formatted_lines: {formatted_lines}')
    return formatted_lines


def read_kbank(kb_tns):
    kbank_date_format = "%d-%m-%y %H:%M"

    # Initialize lists to store the extracted information
    transactions = []

    # Define regular expressions for extracting the information
    pattern = r'(\d{2}-\d{2}-\d{2} \d{2}:\d{2})\s+([A-Za-z\s]+)\s+([\d,]+\.\d{2})\s+(.+?)\s+' \
              r'(Transfer Withdrawal|Transfer Deposit|Trade Finance Deposit|Payment|Direct Debit)\s+([\d,]+\.\d{2})'
    regex = re.compile(pattern)

    # Iterate through the data and extract information
    for item in kb_tns:
        matched = regex.search(item)
        if matched:
            date, channel, balance, details, descriptions, amount = matched.groups()

            # Create a tuple with extracted data and append it to the 'expenses' list
            txn_data = (
                None,
                None,
                datetime.now(),
                datetime.strptime(date, kbank_date_format).date().isoformat(),
                channel,
                float(balance.replace(',', '')),
                details,
                descriptions,
                float(amount.replace(',', ''))
            )
            transactions.append(txn_data)

    return transactions


def insert_db_kbank(data):
    create_table_query = """
    CREATE TABLE IF NOT EXISTS "KBANK" (
        id TEXT PRIMARY KEY,
        matched TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
        date DATE NOT NULL,
        channel TEXT,
        balance NUMERIC NOT NULL,
        details TEXT,
        descriptions TEXT,
        amount NUMERIC NOT NULL   
    )
    """
    # Execute the SQL command to create the table
    cursor.execute(create_table_query)

    # SQL command to insert data into the "KCC" table
    insert_data_query = """
    INSERT INTO "KBANK" (id, matched, created_at, date, channel, balance, details, descriptions, amount)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    # Execute the SQL command for each row
    for row in data:
        row = (f"kb{str(uuid.uuid4().int)}", *row[1:])
        cursor.execute(insert_data_query, row)

    # Commit the changes
    conn.commit()


def update_kbank(in_path, pw, out_path):
    out_path = extract_pdf(in_path, pw, out_path)
    cleaned_data = clean_kbank(out_path)
    kbank_data = read_kbank(cleaned_data)
    insert_db_kbank(kbank_data)


def match():
    # Load data from the database table into a Pandas DataFrame
    df_kcc = pd.DataFrame(pd.read_sql_query('SELECT id, matched, amount FROM KCC WHERE matched IS NULL', conn))
    df_kbank = pd.DataFrame(pd.read_sql_query('SELECT id, matched, amount FROM KBANK WHERE matched IS NULL', conn))

    amount_kcc = df_kcc['amount'].tolist()
    amount_kbank = df_kbank['amount'].tolist()
    # print(f"df_kcc:\n{df_kcc}")
    # print(f"amount_kcc:\n{amount_kcc}")
    # print(f"df_kbank:\n{df_kbank}")
    # print(f"amount_kbank:\n{amount_kbank}")

    unique_kcc = [x for x in amount_kcc if amount_kcc.count(x) == 1]
    unique_kbank = [x for x in amount_kbank if amount_kbank.count(x) == 1]
    common_values = [x for x in unique_kcc if x in unique_kbank]
    # print(f"common_values:{common_values}")

    for value in common_values:
        # Filter row in kcc
        row_kcc = df_kcc[df_kcc['amount'] == value]
        # print(f"filtered_df:{row_kcc}")
        kc_id = row_kcc['id'].tolist()[0]
        # print(f"kc_id:{kc_id}")

        # Filter row in kbank
        row_kbank = df_kbank[df_kbank['amount'] == value]
        # print(f"filtered_df:{row_kbank}")
        kb_id = row_kbank['id'].tolist()[0]
        # print(f"kb_id:{kb_id}")

        # Update KCC
        update_query_kc = "UPDATE KCC SET matched = ? WHERE id = ?;"
        conn.execute(update_query_kc, (kb_id, kc_id))

        # Update KBANK
        update_query_kb = "UPDATE KBANK SET matched = ? WHERE id = ?;"
        conn.execute(update_query_kb, (kc_id, kb_id))

        # Commit the changes
        conn.commit()

    # Load data from the database table into a Pandas DataFrame
    df_kcc = pd.DataFrame(pd.read_sql_query('SELECT id, matched, transaction_date, amount FROM KCC '
                                            'WHERE matched IS NULL', conn))
    df_kbank = pd.DataFrame(pd.read_sql_query('SELECT id, matched, date, amount FROM KBANK '
                                              'WHERE matched IS NULL', conn))
    # print(f"df_kcc:\n{df_kcc}")
    # print(f"df_kbank:\n{df_kbank}")

    amount_kcc = df_kcc['amount'].tolist()
    date_kcc = df_kcc['transaction_date'].tolist()
    id_kcc = df_kcc['id'].tolist()
    amount_kbank = df_kbank['amount'].tolist()
    date_kbank = df_kbank['date'].tolist()
    id_kbank = df_kbank['id'].tolist()

    print(f"count amount_kcc: {len(amount_kcc)} | amount_kbank: {len(amount_kbank)}")
    for i in range(len(amount_kcc)):
        amt_kc, dt_kc, id_kc = amount_kcc[i], date_kcc[i], id_kcc[i]

        for j in range(len(amount_kbank)):
            print(f"i -> j: {i} -> {j}")
            amt_kb, dt_kb, id_kb = amount_kbank[j], date_kbank[j], id_kbank[j]

            if (amt_kc == amt_kb) and (dt_kc == dt_kb):
                print(f"amount_kcc: {amount_kcc}")
                print(f"amount_kbank: {amount_kbank}")

                print(f"[MATCHED] kc: {amt_kc} {dt_kc} | kb: {amt_kb} {dt_kb}")

                # Update KCC
                update_query_kc = "UPDATE KCC SET matched = ? WHERE id = ?;"
                conn.execute(update_query_kc, (id_kb, id_kc))

                # Update KBANK
                update_query_kb = "UPDATE KBANK SET matched = ? WHERE id = ?;"
                conn.execute(update_query_kb, (id_kc, id_kb))

                # Commit the changes
                conn.commit()

                # amount_kcc.pop(i)
                # date_kcc.pop(i)
                # id_kcc.pop(i)
                amount_kbank.pop(j)
                date_kbank.pop(j)
                id_kbank.pop(j)
                break

    print(f"count amount_kcc: {len(amount_kcc)} | amount_kbank: {len(amount_kbank)}")


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # Update KCC statement
    update_kcc(r"C:\Users\miumi\Downloads\credit-card-statement.PDF",
               KCC_PW,
               "kcc_2401.txt")
    # Update Kbank statement
    update_kbank(r"C:\Users\miumi\Downloads\bank-account-statement.pdf",
                 K_BANK_PW,
                 "kbank_2401.txt")
    match()
    conn.close()  # close the connection
