
import sqlite3
import pandas as pd

connection = sqlite3.connect('S30 ETL Assignment.db')


# Select the required columns from the tables 
query = '''
SELECT c.customer_id, c.age, i.item_name, o.quantity
FROM Orders o
INNER JOIN Sales s 
	ON o.sales_id = s.sales_id 
INNER JOIN Customers c 
	ON s.customer_id = c.customer_id 
INNER JOIN Items i 
	ON o.item_id = i.item_id 
'''

tb = pd.read_sql_query(query, connection)


# Filter the records to return customers whose ages are between 18 and 35
filter_tb = tb[(tb['age'] >= 18) & (tb['age'] <= 35)]


# Calculate the total quantity for each customer and item, then rename the columns to the specified headers
result_set = filter_tb.groupby(['customer_id', 'age', 'item_name'], as_index=False) \
						.agg({'quantity': 'sum'}) \
						.rename(columns={'customer_id': 'Customer',
										'age': 'Age',
										'item_name': 'Item',
										'quantity': 'Quantity'})


# Set Quantity column to integer to remove decimals
result_set['Quantity'] = result_set['Quantity'].astype(int)


# Filter the final result to omit total quantity=0
final_result_set = result_set[result_set['Quantity'] > 0]
print(final_result_set)


final_result_set.to_csv('pandas-result.csv', index=False, sep=';')


connection.close()
