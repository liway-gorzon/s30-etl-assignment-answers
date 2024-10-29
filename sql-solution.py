import sqlite3

connection = sqlite3.connect('S30 ETL Assignment.db')
cursor = connection.cursor()

# Select the required columns from the tables. Rename columns to their aliases.
# Filter the result to return customers with ages 18 to 35 and excluding total quantity = 0.
cursor.execute('''
	SELECT c.customer_id as `Customer`, c.age as `Age`, i.item_name as `Item`, SUM(o.quantity) as `Quantity` 
	FROM Orders o 
	JOIN Sales s 
		ON o.sales_id = s.sales_id 
	JOIN Customers c 
		ON s.customer_id = c.customer_id 
	JOIN Items i 
		ON o.item_id = i.item_id 
	WHERE c.age BETWEEN 18 AND 35 
	GROUP BY c.customer_id, c.age, o.item_id
	HAVING SUM(o.quantity) > 0
	''')

rows = cursor.fetchall()

# Add the column headers in the result
header = [description[0] for description in cursor.description]

# Setting the delimiter/separator to ";"
result = []
result.append(';'.join(header))
for r in rows:
	result.append(';'.join(map(str, r)))

# Print the result set
for i in result:
	print(i)


connection.close()
