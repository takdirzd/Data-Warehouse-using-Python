

# CREATE TABLE DATA WAREHOUSE

CREATE TABLE err_log (
    PROC_NAME VARCHAR(20),
    LOG_NO INT AUTO_INCREMENT NOT NULL, -- Place AUTO_INCREMENT after INT and before NOT NULL
    LOG_DATE DATE,
    ERR_MSG VARCHAR(4000),
    PRIMARY KEY (LOG_NO) -- Define LOG_NO as the primary key
);


CREATE UNIQUE INDEX XPKERR_LOG ON ERR_LOG
(LOG_NO);

ALTER TABLE ERR_LOG ADD (
  CONSTRAINT XPKERR_LOG
  PRIMARY KEY
  (LOG_NO)
  USING INDEX XPKERR_LOG
  ENABLE VALIDATE);
  
 
 //BUAT TABEL PRODUCT

CREATE TABLE product (
    PRODUCT_ID CHAR(15),
    CATEGORY VARCHAR(20),
    SUB_CATEGORY VARCHAR(20),
    PRODUCT_NAME VARCHAR(130)
)

ALTER TABLE product
ADD PRIMARY KEY (PRODUCT_ID);

CREATE TABLE location (
    ZIPCODE INT(6) NOT NULL,
    COUNTRY VARCHAR(20),
    REGION VARCHAR(10) NOT NULL,
    STATE VARCHAR(30),
    CITY VARCHAR(30)
);

CREATE UNIQUE INDEX idx_location_zipcode ON location (ZIPCODE);


ALTER TABLE location ADD CONSTRAINT XPKLOCATION PRIMARY KEY (ZIPCODE);

ALTER TABLE location
ADD CONSTRAINT REGION_FK
FOREIGN KEY (REGION)
REFERENCES region_mgr(REGION)
ON DELETE SET NULL;



//BUAT TABEL CUSTOMER

CREATE TABLE customer (
    CUSTOMER_ID CHAR(8),
    CUSTOMER_NAME VARCHAR(30),
    SEGMENT VARCHAR(20)
)

ALTER TABLE customer
ADD PRIMARY KEY (CUSTOMER_ID);



// BUAT TABEL REGION MGR

CREATE TABLE region_mgr (
    Person VARCHAR(30),
    Region VARCHAR(10)
);

CREATE UNIQUE INDEX XPKREGION_MGR ON REGION_MGR (REGION);

ALTER TABLE region_mgr 
ADD CONSTRAINT XPKREGION_MGR PRIMARY KEY USING INDEX XPKREGION_MGR (Region);



//BUAT TABEL ORDERS

CREATE TABLE orders (
    ORDER_Date DATE,
    ORDER_ID CHAR(14),
    PRODUCT_ID CHAR(15),
    CUSTOMER_ID CHAR(8),
    SHIP_DATE DATE,
    ZIPCODE INT(6),
    SALES DECIMAL(10,2),
    QUANTITY INT,
    DISCOUNT DECIMAL(5,2),
    PROFIT DECIMAL(10,2),
    RETURNED CHAR(3)
);

ALTER TABLE orders
ADD CONSTRAINT fk_product FOREIGN KEY (PRODUCT_ID) REFERENCES product(PRODUCT_ID);

ALTER TABLE orders
ADD CONSTRAINT fk_customer FOREIGN KEY (CUSTOMER_ID) REFERENCES customer(CUSTOMER_ID);

ALTER TABLE orders
ADD CONSTRAINT fk_zip FOREIGN KEY (ZIPCODE) REFERENCES location(ZIPCODE);

ALTER TABLE orders
ADD CONSTRAINT pk_order_id PRIMARY KEY (order_id, order_date, product_id);