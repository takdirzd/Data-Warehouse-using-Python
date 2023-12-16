
# CREATE TABLE STAGING

create table Superstore(
Row_ID	          INTEGER,
Order_ID	        VARCHAR(14),
Order_Date	      DATE,
Ship_Date	        DATE,
Ship_Mode	        VARCHAR(15),
Customer_ID	      VARCHAR(8),
Customer_Name	    VARCHAR(30),
Segment	          VARCHAR(15),
Country	          VARCHAR(20),
City	            VARCHAR(30),
State	            VARCHAR(30),
Postal_Code	      INTEGER(6),
Region	          VARCHAR(10),
Product_ID	      VARCHAR(15),
Category	        VARCHAR(20),
Sub_Category	    VARCHAR(20),
Product_Name	    VARCHAR(130),
Sales	            DECIMAL(10,2),
Quantity	        INTEGER,
Discount	        DECIMAL(6,2),
Profit            DECIMAL(10,2));

Create table PEOPLE(
Person	VARCHAR(30),
Region  VARCHAR(10));

Create table RETURNS(
Order_ID	VARCHAR(14),
Returned  VARCHAR(3)
); 