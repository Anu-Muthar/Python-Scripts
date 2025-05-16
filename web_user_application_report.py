import psycopg2
from postmarker.core import PostmarkClient
from datetime import datetime, timedelta

# Connect to PostgreSQL database (dummy credentials used)
con = psycopg2.connect(
    database="your_database_name",
    user="your_username",
    password="your_password",
    host="your_host_address",
    port="5432"
)

print("Database connection established successfully.")
cur = con.cursor()

# Get yesterday's date for report naming
date = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')

# SQL query to export user applications between specific dates
social_sql = """
COPY (
    WITH report AS (
        SELECT 
            ua.id,
            ua.created_at AS created_at,
            ua.first_name,
            ua.last_name,
            ua.display_field,
            ua.alias,
            CASE 
                WHEN display_field = 'first_name' THEN COALESCE(first_name, '') || ' ' || COALESCE(last_name, '')
                WHEN display_field = 'last_name' THEN COALESCE(last_name, '') || ' ' || COALESCE(first_name, '')
                ELSE COALESCE(alias, '')
            END AS display_name,
            ua.email,
            ua.created_at AS applied_date
        FROM user_applications ua
    )
    SELECT display_name, email, applied_date
    FROM report
    WHERE DATE(created_at::date) BETWEEN '2022-07-12' AND '2022-07-19'
    ORDER BY created_at
) TO STDOUT WITH CSV DELIMITER ','
"""

# Write CSV file
with open("webusers-" + date + ".csv", "w", encoding='utf-8') as file_social:
    file_social.write('display_name,email,applied_date\n')  # CSV headers
    cur.copy_expert(social_sql, file_social)

# Set up Postmark client (dummy server token)
postmark = PostmarkClient(server_token='your_postmark_server_token')

# Create and send email with attachment
email = postmark.emails.Email(
    From='sender@example.com',
    To='recipient@example.com',
    Subject='Web User Report ' + date,
    HtmlBody="Attached is the latest user report."
)

# Attach generated CSV and send
email.attach("webusers-" + date + ".csv")
email.send()

print(str(datetime.now()) + ' - Report sent successfully.')
