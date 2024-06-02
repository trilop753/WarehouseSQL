-- Author: Maroš Pavlík

-- sequences for ids
CREATE SEQUENCE warehouse_seq START WITH 1 INCREMENT BY 1;
CREATE SEQUENCE product_seq START WITH 1 INCREMENT BY 1;
CREATE SEQUENCE transaction_log_seq START WITH 1 INCREMENT BY 1;

-- creating table of warehouse
CREATE TABLE Warehouse (
    warehouse_id INT DEFAULT warehouse_seq.NEXTVAL PRIMARY KEY,
    warehouse_name VARCHAR(25) NOT NULL,
    location VARCHAR(35) NOT NULL,
    product_types VARCHAR(105) NOT NULL,
    capacity INT NOT NULL
);

-- creating table of product
CREATE TABLE Product (
    product_id INT DEFAULT product_seq.NEXTVAL PRIMARY KEY,
    product_type VARCHAR(25) NOT NULL,
    quantity INT NOT NULL,
    warehouse_id INT NOT NULL,
    FOREIGN KEY (warehouse_id) REFERENCES Warehouse(warehouse_id)
);

-- creating table of transaction logs
CREATE TABLE Transaction_Log (
    transaction_id INT DEFAULT transaction_log_seq.NEXTVAL PRIMARY KEY,
    transaction_type VARCHAR(6) CHECK (transaction_type IN ('IMPORT', 'EXPORT')) NOT NULL,
    product_type VARCHAR(25) NOT NULL,
    warehouse_id INT NOT NULL,
    quantity_changed INT NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    FOREIGN KEY (warehouse_id) REFERENCES Warehouse(warehouse_id)
);

----------------------------------------------------------------------

-- trigger to log the import of product to the transaction log
CREATE OR REPLACE TRIGGER import_product_log_trigger AFTER INSERT ON Product FOR EACH ROW
BEGIN
    INSERT INTO Transaction_Log (transaction_type, product_type, warehouse_id, quantity_changed, timestamp)
    VALUES ('IMPORT', :NEW.product_type, :NEW.warehouse_id, :NEW.quantity, SYSTIMESTAMP);
END;
/

-- trigger to log the export of product to the transaction log
CREATE OR REPLACE TRIGGER export_product_log_trigger BEFORE DELETE ON Product FOR EACH ROW
BEGIN
    INSERT INTO Transaction_Log (transaction_type, product_type, warehouse_id, quantity_changed, timestamp)
    VALUES ('EXPORT', :OLD.product_type, :OLD.warehouse_id, :OLD.quantity, SYSTIMESTAMP);
END;
/

-- trigger checking if the product type can be stored in warehouse
CREATE OR REPLACE TRIGGER check_product_type_trigger BEFORE INSERT ON Product FOR EACH ROW
DECLARE
    warehouse_product_types VARCHAR2(100);
BEGIN
    SELECT product_types INTO warehouse_product_types
    FROM Warehouse
    WHERE warehouse_id = :NEW.warehouse_id;

    IF warehouse_product_types IS NOT NULL AND warehouse_product_types NOT LIKE '%' || :NEW.product_type || '%' THEN
        RAISE_APPLICATION_ERROR(-20001, 'Product type is not allowed in this warehouse');
    END IF;
END;
/

-- trigger checking if the product quantity would exceed the capacity of warehouse
CREATE OR REPLACE TRIGGER check_capacity_trigger BEFORE INSERT ON Product FOR EACH ROW
DECLARE
    total_quantity INT;
    warehouse_capacity INT;
BEGIN
    SELECT COALESCE(SUM(quantity), 0)
    INTO total_quantity
    FROM Product
    WHERE warehouse_id = :NEW.warehouse_id;

    SELECT capacity
    INTO warehouse_capacity
    FROM Warehouse
    WHERE warehouse_id = :NEW.warehouse_id;

    IF total_quantity + :NEW.quantity > warehouse_capacity THEN
        RAISE_APPLICATION_ERROR(-20001, 'Adding this quantity would exceed warehouse capacity');
    END IF;
END;
/

----------------------------------------------------------------------
-- Inserting Warehouse A
INSERT INTO Warehouse (warehouse_name, location, product_types, capacity)
VALUES ('Warehouse A', 'Location A', 'Plants, Clothing, Meat', 125);

-- Inserting Warehouse B
INSERT INTO Warehouse (warehouse_name, location, product_types, capacity)
VALUES ('Warehouse B', 'Location B', 'Books, Toys, Furniture', 300);

-- Inserting Warehouse C
INSERT INTO Warehouse (warehouse_name, location, product_types, capacity)
VALUES ('Warehouse C', 'Location C', 'Electronics, Tools, Appliances', 250);


-- Importing Clothing into Warehouse A
INSERT INTO Product (product_type, quantity, warehouse_id)
VALUES ('Clothing', 50, 1);

-- Importing Meat into Warehouse A
INSERT INTO Product (product_type, quantity, warehouse_id)
VALUES ('Meat', 40, 1);

-- Importing Toys into Warehouse B
INSERT INTO Product (product_type, quantity, warehouse_id)
VALUES ('Toys', 300, 2);

-- Importing Tools into Warehouse C
INSERT INTO Product (product_type, quantity, warehouse_id)
VALUES ('Tools', 100, 3);


-- check logs
SELECT * FROM Transaction_Log ORDER BY transaction_id;

-- capacity error
INSERT INTO Product (product_type, quantity, warehouse_id)
VALUES ('Plants', 200, 1);

-- product type error
INSERT INTO Product (product_type, quantity, warehouse_id)
VALUES ('Books', 10, 3);

-- exporting products
DELETE FROM Product
WHERE product_id = 2;

DELETE FROM Product
WHERE product_id = 3;

-- check logs
SELECT * FROM Transaction_Log ORDER BY transaction_id;

----------------------------------------------------------------------

-- function to print all products in warehouse with given id (with cursor)
CREATE OR REPLACE FUNCTION get_products_in_warehouse(p_warehouse_id INT) RETURN VARCHAR2
IS
    CURSOR product_cursor IS
        SELECT product_id, product_type, quantity FROM Product WHERE warehouse_id = p_warehouse_id;

    product_info VARCHAR2(500);
    cursor_product_id Product.product_id%TYPE;
    cursor_product_type Product.product_type%TYPE;
    cursor_quantity Product.quantity%TYPE;
BEGIN
    OPEN product_cursor;
    product_info := '';
    LOOP
        FETCH product_cursor INTO cursor_product_id, cursor_product_type, cursor_quantity;
        EXIT WHEN product_cursor%NOTFOUND;

        product_info := product_info || 'Product ID: ' || cursor_product_id || ', Product Type: ' || cursor_product_type || ', Quantity Available: ' || cursor_quantity || CHR(10);
    END LOOP;
    CLOSE product_cursor;
    RETURN product_info;
END;
/

INSERT INTO Product (product_type, quantity, warehouse_id)
VALUES ('Plants', 15, 1);

SELECT get_products_in_warehouse(1) FROM DUAL;
----------------------------------------------------------------------

-- select count of products and free capacity left for each warehouse
SELECT 
    w.warehouse_id,
    w.warehouse_name,
    COUNT(p.product_id) AS product_count,
    w.capacity AS total_capacity,
    w.capacity - COALESCE(SUM(p.quantity), 0) AS free_capacity
FROM 
    Warehouse w
LEFT JOIN 
    Product p ON w.warehouse_id = p.warehouse_id
GROUP BY 
    w.warehouse_id,
    w.warehouse_name,
    w.capacity;

-- select warehouse id with the most imported and exported products
SELECT 
    warehouse_id,
    COUNT(*) AS total_transactions
FROM 
    Transaction_Log
GROUP BY 
    warehouse_id
ORDER BY 
    COUNT(*) DESC
FETCH FIRST 1 ROW ONLY;

-- select warehouse name, product with the highest quantity and total filled capacity for each warehouse
SELECT 
    w.warehouse_name,
    p.product_type AS product_with_highest_quantity,
    (SELECT SUM(quantity)
     FROM Product 
     WHERE warehouse_id = w.warehouse_id) AS total_quantity
FROM 
    Warehouse w
LEFT JOIN 
    Product p ON w.warehouse_id = p.warehouse_id
WHERE 
    p.quantity = (SELECT MAX(quantity) 
                            FROM Product 
                            WHERE warehouse_id = w.warehouse_id);
