/****** Script for SelectTopNRows command from SSMS  ******/

-- Preparatory tasks

SELECT *
FROM Snapshot_Raw_1206

UPDATE Transactions_1206
SET Amount = ROUND(Amount,2)

UPDATE Transactions_1206
SET LotName = NULL
WHERE LotName = 'NULL'

SELECT
FROM Transactions_1206 t
INNER JOIN Lot_Details l
ON t.LotName = l.LotName

-- Total amounts by lot and transaction type, row by row
SELECT LotName, TransactionType, SUM(Amount) AS Totals
FROM Transactions_1206
GROUP BY TransactionType, LotName

-- Load the above query into a view
CREATE VIEW Snapshot_Raw_1206 AS
SELECT LotName, TransactionType, SUM(Amount) AS Totals
FROM Transactions_1206
GROUP BY TransactionType, LotName

-- Use the pivot function to condense the 'Raw' table while taking care of NULLs
SELECT LotName,
[revenue] AS Revenue,
[purchase] AS Purchases,
[fee] AS Fees,
[shipseller] AS ShipSeller,
[shipbuyer] AS ShipBuyer
FROM 
   ( SELECT LotName, TransactionType, Amount
   FROM Transactions_1206
   ) Pivot_1206
PIVOT
   ( SUM (Amount)
     FOR TransactionType IN ( [revenue],
	 [purchase],
	 [fee],
	 [shipseller],
	 [shipbuyer])
   ) AS pvt

   -- Load the above query into a view
CREATE VIEW Snapshot_Expanded_1206 AS
SELECT LotName,
[revenue] AS Revenue,
[purchase] AS Purchases,
[fee] AS Fees,
[shipseller] AS ShipSeller,
[shipbuyer] AS ShipBuyer
FROM 
   (SELECT LotName, TransactionType, Amount
   FROM Transactions_1206
   ) Pivot_1206
PIVOT
   ( SUM (Amount)
     FOR TransactionType IN ( [revenue],
	 [purchase],
	 [fee],
	 [shipseller],
	 [shipbuyer])
   ) AS pvt
   
   -- Convert NULLs into zeros in the 'Expanded' table
   -- Can't do that in a view because they are derived values, so load them into a table instead
SELECT LotName,
[revenue] AS Revenue,
[purchase] AS Purchases,
[fee] AS Fees,
[shipseller] AS ShipSeller,
[shipbuyer] AS ShipBuyer
INTO Snapshot_Condensed_1206
FROM 
   (SELECT LotName, TransactionType, Amount
   FROM Transactions_1206
   ) Pivot_1206
PIVOT
   ( SUM (Amount)
     FOR TransactionType IN ( [revenue],
	 [purchase],
	 [fee],
	 [shipseller],
	 [shipbuyer])
   ) AS pvt

   SELECT *
   FROM Snapshot_Condensed_1206

   -- Now convert NULLS into zeros
   UPDATE Snapshot_Condensed_1206
   SET Revenue = 0
   WHERE Revenue IS NULL

   UPDATE Snapshot_Condensed_1206
   SET Purchases = 0
   WHERE Purchases IS NULL

   UPDATE Snapshot_Condensed_1206
   SET Fees = 0
   WHERE Fees IS NULL

      UPDATE Snapshot_Condensed_1206
   SET ShipBuyer = 0
   WHERE ShipBuyer IS NULL

      UPDATE Snapshot_Condensed_1206
   SET ShipSeller = 0
   WHERE ShipSeller IS NULL

SELECT *
FROM Snapshot_Condensed_1206

-- Now we add the Profit column, calculate it for each lot, and round the values
ALTER TABLE Snapshot_Condensed_1206
ADD Profit float

UPDATE Snapshot_Condensed_1206
SET Profit = Revenue-Purchases-Fees-ShipSeller-ShipBuyer

UPDATE Snapshot_Condensed_1206
SET Profit = ROUND(Profit,2)

SELECT *
FROM Snapshot_Condensed_1206

-- What is our total profit/loss?
SELECT SUM(Profit) AS TOTAL
FROM Snapshot_Condensed_1206

-- Create views for simplified buckets (revenue, profit, all expenses)
-- Start with a temp table
CREATE TABLE #temp_simple (
LotName varchar(100),
Revenue float,
Purchases float,
Fees float,
ShipSeller float,
ShipBuyer float,
Profit float,
AllExpenses float
)

SELECT *
FROM #temp_simple

INSERT INTO #temp_simple
SELECT *, (Purchases+Fees+ShipSeller+ShipBuyer) AS AllExpenses
FROM Snapshot_Condensed_1206

-- Now use the temp table to create a more simplified view
DROP VIEW IF EXISTS Snapshot_Simple_1206

CREATE VIEW Snapshot_Simple_1206 AS
SELECT LotName, Revenue, AllExpenses, Profit
FROM #temp_simple

-- Can't use a temp table to create a view
CREATE VIEW Snapshot_AllExpenses_1206 AS
SELECT *, (Purchases+Fees+ShipSeller+ShipBuyer) AS AllExpenses
FROM Snapshot_Condensed_1206

SELECT *
FROM Snapshot_AllExpenses_1206

CREATE VIEW Snapshot_Simple_1206 AS
SELECT LotName, Revenue,AllExpenses,Profit
FROM Snapshot_AllExpenses_1206

SELECT *
FROM Snapshot_Simple_1206

-- Now let's move on to analysis for individual item performance
SELECT ItemName,
[revenue] AS Revenue,
[purchase] AS Purchases,
[fee] AS Fees,
[shipseller] AS ShipSeller,
[shipbuyer] AS ShipBuyer
INTO Snapshot_Items_1206
FROM 
   ( SELECT ItemName, TransactionType, Amount
   FROM Transactions_1206
   ) Pivot_1206
PIVOT
   ( SUM (Amount)
     FOR TransactionType IN ( [revenue],
	 [purchase],
	 [fee],
	 [shipseller],
	 [shipbuyer])
   ) AS pvt

 UPDATE Snapshot_Items_1206
   SET Purchases = 0
   WHERE Purchases IS NULL

SELECT *
FROM Snapshot_Items_1206

SELECT ItemName, Profit
FROM Snapshot_Items_1206
ORDER BY Profit DESC

-- Lastly, let's see how much of each lot is sold and remains to be sold
CREATE TABLE #temp_sales (LotName varchar, ItemsSold int);
INSERT INTO #temp_sales

CREATE VIEW Sales_Count_1206 AS
SELECT LotName, COUNT(TransactionID) AS ItemsSold
FROM Transactions_1206
WHERE TransactionType = 'revenue' and ItemType = 'doll'
GROUP BY LotName
--ORDER BY ItemsSold DESC
SELECT * FROM Sales_Count_1206

CREATE VIEW Purchases_Count_1206 AS
SELECT LotName, COUNT(TransactionID) AS ItemsPurchased
FROM Transactions_1206
WHERE TransactionType = 'purchase' and ItemType = 'doll'
GROUP BY LotName
--ORDER BY ItemsPurchased DESC

CREATE VIEW LotCount_Items_1206 AS
SELECT p.LotName, ItemsPurchased, ItemsSold
FROM Purchases_Count_1206 p
INNER JOIN Sales_Count_1206 s
ON p.LotName = s.LotName

SELECT * FROM LotCount_Items_1206

-- Investigating 3rd Auction because there are more sold dolls than purchased dolls
SELECT LotName, TransactionID, ItemName
FROM Transactions_1206
WHERE LotName = '3rd auction' AND TransactionType = 'revenue' AND ItemType ='doll'
--GROUP BY LotName
ORDER BY ItemName

SELECT LotName, TransactionID, ItemName
FROM Transactions_1206
WHERE LotName = '3rd auction' AND TransactionType = 'purchase' AND ItemType ='doll'
--GROUP BY LotName
ORDER BY ItemName
