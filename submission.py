
import mysql.connector

mydb_con = mysql.connector.connect(
  host="localhost",
  user="root",
  password="Emre4553",
  port='3306',auth_plugin='mysql_native_password'
)


cursor = mydb_con.cursor()
cursor.execute("CREATE DATABASE little_lemon_db") 
cursor.execute("USE little_lemon_db")

#MenuItems table
create_menuitem_table = """CREATE TABLE MenuItems (
ItemID INT AUTO_INCREMENT,
Name VARCHAR(200),
Type VARCHAR(100),
Price INT,
PRIMARY KEY (ItemID)
);"""


# Create MenuItems table
cursor.execute(create_menuitem_table)


create_menu_table = """CREATE TABLE Menus (
MenuID INT,
ItemID INT,
Cuisine VARCHAR(100),
PRIMARY KEY (MenuID,ItemID)
);"""

# Create Menu table
cursor.execute(create_menu_table)

create_booking_table = """CREATE TABLE Bookings (
BookingID INT AUTO_INCREMENT,
TableNo INT,
GuestFirstName VARCHAR(100) NOT NULL,
GuestLastName VARCHAR(100) NOT NULL,
BookingSlot TIME NOT NULL,
EmployeeID INT,
PRIMARY KEY (BookingID)
);"""

# Create Bookings table
cursor.execute(create_booking_table)

create_orders_table = """CREATE TABLE Orders (
OrderID INT,
TableNo INT,
MenuID INT,
BookingID INT,
BillAmount INT,
Quantity INT,
PRIMARY KEY (OrderID,TableNo)
);"""

# Create Orders table
cursor.execute(create_orders_table)


create_employees_table = """CREATE TABLE Employees (
EmployeeID INT AUTO_INCREMENT PRIMARY KEY,
Name VARCHAR(100),
Role VARCHAR(100),
Address VARCHAR(100),
Contact_Number INT,
Email VARCHAR(100),
Annual_Salary VARCHAR(100)
);"""
# Create Employees table
cursor.execute(create_employees_table)







#*******************************************************#
# Insert query to populate "MenuItems" table:
#*******************************************************#
insert_menuitems="""
INSERT INTO MenuItems (ItemID, Name, Type, Price)
VALUES
(1, 'Olives','Starters',5),
(2, 'Flatbread','Starters', 5),
(3, 'Minestrone', 'Starters', 8),
(4, 'Tomato bread','Starters', 8),
(5, 'Falafel', 'Starters', 7),
(6, 'Hummus', 'Starters', 5),
(7, 'Greek salad', 'Main Courses', 15),
(8, 'Bean soup', 'Main Courses', 12),
(9, 'Pizza', 'Main Courses', 15),
(10, 'Greek yoghurt','Desserts', 7),
(11, 'Ice cream', 'Desserts', 6),
(12, 'Cheesecake', 'Desserts', 4),
(13, 'Athens White wine', 'Drinks', 25),
(14, 'Corfu Red Wine', 'Drinks', 30),
(15, 'Turkish Coffee', 'Drinks', 10),
(16, 'Turkish Coffee', 'Drinks', 10),
(17, 'Kabasa', 'Main Courses', 17);"""

#*******************************************************#
# Insert query to populate "Menu" table:
#*******************************************************#
insert_menu="""
INSERT INTO Menus (MenuID,ItemID,Cuisine)
VALUES
(1, 1, 'Greek'),
(1, 7, 'Greek'),
(1, 10, 'Greek'),
(1, 13, 'Greek'),
(2, 3, 'Italian'),
(2, 9, 'Italian'),
(2, 12, 'Italian'),
(2, 15, 'Italian'),
(3, 5, 'Turkish'),
(3, 17, 'Turkish'),
(3, 11, 'Turkish'),
(3, 16, 'Turkish');"""

#*******************************************************#
# Insert query to populate "Bookings" table:
#*******************************************************#
insert_bookings="""
INSERT INTO Bookings (BookingID, TableNo, GuestFirstName, 
GuestLastName, BookingSlot, EmployeeID)
VALUES
(1, 12, 'Anna','Iversen','19:00:00',1),
(2, 12, 'Joakim', 'Iversen', '19:00:00', 1),
(3, 19, 'Vanessa', 'McCarthy', '15:00:00', 3),
(4, 15, 'Marcos', 'Romero', '17:30:00', 4),
(5, 5, 'Hiroki', 'Yamane', '18:30:00', 2),
(6, 8, 'Diana', 'Pinto', '20:00:00', 5);"""

#*******************************************************#
# Insert query to populate "Orders" table:
#*******************************************************#
insert_orders="""
INSERT INTO Orders (OrderID, TableNo, MenuID, BookingID, Quantity, BillAmount)
VALUES
(1, 12, 1, 1, 2, 86),
(2, 19, 2, 2, 1, 37),
(3, 15, 2, 3, 1, 37),
(4, 5, 3, 4, 1, 40),
(5, 8, 1, 5, 1, 43);"""

#*******************************************************#
# Insert query to populate "Employees" table:
#*******************************************************#
insert_employees = """
INSERT INTO employees (EmployeeID, Name, Role, Address, Contact_Number, Email, Annual_Salary)
values
(01,'Mario Gollini','Manager','724, Parsley Lane, Old Town, Chicago, IL',351258074,'Mario.g@littlelemon.com','$70,000'),
(02,'Adrian Gollini','Assistant Manager','334, Dill Square, Lincoln Park, Chicago, IL',351474048,'Adrian.g@littlelemon.com','$65,000'),
(03,'Giorgos Dioudis','Head Chef','879 Sage Street, West Loop, Chicago, IL',351970582,'Giorgos.d@littlelemon.com','$50,000'),
(04,'Fatma Kaya','Assistant Chef','132  Bay Lane, Chicago, IL',351963569,'Fatma.k@littlelemon.com','$45,000'),
(05,'Elena Salvai','Head Waiter','989 Thyme Square, EdgeWater, Chicago, IL',351074198,'Elena.s@littlelemon.com','$40,000'),
(06,'John Millar','Receptionist','245 Dill Square, Lincoln Park, Chicago, IL',351584508,'John.m@littlelemon.com','$35,000');"""



# Populate MenuItems table
cursor.execute(insert_menuitems)
mydb_con.commit()

# Populate MenuItems table
cursor.execute(insert_menu)
mydb_con.commit()

# Populate Bookings table
cursor.execute(insert_bookings)
mydb_con.commit()

# Populate Orders table
cursor.execute(insert_orders)
mydb_con.commit()

# Populate Employees table
cursor.execute(insert_employees)
mydb_con.commit()

from mysql.connector.pooling import MySQLConnectionPool

try:
  # Define the database connection pool
  pool_config = {
      "pool_name": "little_lemon_pool",
      "pool_size": 2,
      "host": "localhost",
      "user": "root",
      "password": "Emre4553",
      "database": "little_lemon_db"
  }

  connection_pool = MySQLConnectionPool(**pool_config)

  # Acquire two connections from the pool
  connection1 = connection_pool.get_connection()
  connection2 = connection_pool.get_connection()

  if connection1 and connection2:
      print("Pool 'pool_a' with two connections created successfully")

      # Now you can use connection1 and connection2 for your database operations.
      # Remember to release the connections back to the pool when done using them.

  else:
      print("Failed to acquire connections from the pool.")
except errors.PoolError as pe:
    print(f"Pool error: {pe}")

peakhours="""CREATE DEFINER=`root`@`localhost` PROCEDURE `PeakHours`()
BEGIN
    -- Declare variables
    DECLARE peak_hour INT;
    DECLARE max_bookings INT;

		SELECT hour, max_bookings_
    INTO peak_hour, max_bookings
    FROM (
        SELECT HOUR(BookingSlot) AS hour, COUNT(*) AS max_bookings_
        FROM bookings
        GROUP BY hour
        ORDER BY max_bookings DESC
        LIMIT 1
    ) AS peak_hours_query;

    -- Return the peak hour and the number of bookings
    SELECT peak_hour AS 'Peak Hour', max_bookings AS 'Number of Bookings';
END;"""

connection1_cursor=connection1.cursor()
connection1_cursor.execute("drop procedure PeakHours")
connection1_cursor.execute(peakhours)

connection1_cursor.callproc('PeakHours')
result=next(connection1_cursor.stored_results())
dataset=result.fetchall()

print(result.column_names)

for data in dataset:
    print(data)



query ="""CREATE PROCEDURE GuestStatus()
BEGIN
    -- Step two: Combine the guest's first and last name
    SELECT CONCAT(GuestLastName, ' ', GuestLastName) AS GuestName,
    
    -- Step three: Use CASE to implement different statuses based on employee's role
    CASE
        WHEN E.Role IN ('Manager', 'Assistant Manager') THEN 'Ready to pay'
        WHEN E.Role = 'Head Chef' THEN 'Ready to serve'
        WHEN E.Role = 'Assistant Chef' THEN 'Preparing Order'
        WHEN E.Role = 'Head Waiter' THEN 'Order served'
        ELSE 'Unknown'
    END AS GuestStatus
    
    -- Step four: LEFT JOIN Bookings table with Employees ON EmployeeID
    FROM Bookings B
    LEFT JOIN Employees E ON B.EmployeeID = E.EmployeeID;
    
END;"""
    

connection1_cursor=connection2.cursor()
connection1_cursor.execute(query)

connection1_cursor.callproc('GuestStatus')
result=next(connection1_cursor.stored_results())
dataset=result.fetchall()

print(result.column_names)

for data in dataset:
    print(data)