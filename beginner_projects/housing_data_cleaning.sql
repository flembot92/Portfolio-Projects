SELECT TOP (100) *
FROM Housing
------------------------------------------------------------------------------------------------
-- Standardizing the data format by converting into "date" type

SELECT SaleDate, CONVERT(Date,SaleDate)
FROM Housing

UPDATE Housing
SET SaleDate = CONVERT(Date,SaleDate)

-- That didn't work, so let's try this instead
ALTER TABLE Housing
ADD SaleDateConverted date;

UPDATE Housing
SET SaleDateConverted = CONVERT(Date,SaleDate)

SELECT SaleDateConverted
FROM Housing

------------------------------------------------------------------------------------------------
-- Populating the property address data
SELECT *
FROM Housing
WHERE PropertyAddress IS NULL

SELECT *
FROM Housing
ORDER BY ParcelID

-- ParcelID is unique to an address, so we can populate blank addresses based on ParcelID.
-- This query gets us records where the ParcelID is the same, but it's not the same row
SELECT a.ParcelID, a.PropertyAddress, b.ParcelID, b.PropertyAddress
FROM Housing a
JOIN Housing b
ON a.ParcelID = b.ParcelID
AND a.[UniqueID ] <> b.[UniqueID ]
WHERE a.PropertyAddress IS NULL

-- The ISNULL function takes the 1st argument, and if it's NULL, populates it with the 2nd argument
SELECT a.ParcelID, a.PropertyAddress, b.ParcelID, b.PropertyAddress, ISNULL(a.PropertyAddress,b.PropertyAddress)
FROM Housing a
JOIN Housing b
ON a.ParcelID = b.ParcelID
AND a.[UniqueID ] <> b.[UniqueID ]
WHERE a.PropertyAddress IS NULL

-- Now let's replace the NULLs with the address
UPDATE a
SET PropertyAddress = ISNULL(a.PropertyAddress,b.PropertyAddress)
FROM Housing a
JOIN Housing b
ON a.ParcelID = b.ParcelID
AND a.[UniqueID ] <> b.[UniqueID ]
WHERE a.PropertyAddress IS NULL

SELECT *
FROM Housing
WHERE PropertyAddress IS NULL

------------------------------------------------------------------------------------------------
-- Breaking out the address into individual columns (Address, City, State) rather than keeping it all in one cell
SELECT PropertyAddress
FROM Housing

SELECT
SUBSTRING(PropertyAddress, 1, CHARINDEX(',', PropertyAddress)) AS Address
FROM Housing

--This query removes the comma too
SELECT
SUBSTRING(PropertyAddress, 1, CHARINDEX(',', PropertyAddress) -1) AS Address
,SUBSTRING(PropertyAddress, CHARINDEX(',', PropertyAddress) +1, LEN(PropertyAddress)) AS Address
FROM Housing

ALTER TABLE Housing
ADD PropertySplitAddress nvarchar(255);

UPDATE Housing
SET PropertySplitAddress = SUBSTRING(PropertyAddress, 1, CHARINDEX(',', PropertyAddress) -1)

ALTER TABLE Housing
ADD PropertySplitCity nvarchar(255);

UPDATE Housing
SET PropertySplitCity = SUBSTRING(PropertyAddress, CHARINDEX(',', PropertyAddress) +1, LEN(PropertyAddress))

SELECT *
FROM Housing

-- Now do the same for the owner's address, but with the parsename function (with a replace inside because parsename deals with commas by default)
SELECT OwnerAddress
FROM Housing

SELECT
PARSENAME(REPLACE(OwnerAddress,',','.'),3)
,PARSENAME(REPLACE(OwnerAddress,',','.'),2)
,PARSENAME(REPLACE(OwnerAddress,',','.'),1)
FROM Housing

ALTER TABLE Housing
ADD OwnerSplitAddress nvarchar(255);

UPDATE Housing
SET OwnerSplitAddress = PARSENAME(REPLACE(OwnerAddress,',','.'),3)

-- Messed up this one; called it SplitCity then changed it to OwnerSplitCity; remember to delete then see if you can rearrange
ALTER TABLE Housing
ADD OwnerSplitCity nvarchar(255);

UPDATE Housing
SET OwnerSplitCity = PARSENAME(REPLACE(OwnerAddress,',','.'),2)

ALTER TABLE Housing
ADD OwnerSplitState nvarchar(255);

UPDATE Housing
SET OwnerSplitState = PARSENAME(REPLACE(OwnerAddress,',','.'),1)

SELECT *
FROM Housing

------------------------------------------------------------------------------------------------
-- Change Y/N to Yes/No in SoldAsVacant field
SELECT DISTINCT(SoldAsVacant), COUNT(SoldAsVacant)
FROM Housing
GROUP BY SoldAsVacant
ORDER BY 2

SELECT SoldAsVacant
	,CASE
		WHEN SoldAsVacant = 'Y' THEN 'Yes'
		WHEN SoldAsVacant = 'N' THEN 'No'
		ELSE SoldAsVacant
	END
FROM Housing

UPDATE Housing
SET SoldAsVacant = CASE
		WHEN SoldAsVacant = 'Y' THEN 'Yes'
		WHEN SoldAsVacant = 'N' THEN 'No'
		ELSE SoldAsVacant
	END

------------------------------------------------------------------------------------------------
-- Remove duplicates and unused columns
WITH RowNumCTE AS(
SELECT *,
	ROW_NUMBER() OVER (
	PARTITION BY ParcelID,
				PropertyAddress,
				SalePrice,
				SaleDate,
				LegalReference
				ORDER BY
					UniqueID
					) AS RowNum
FROM Housing
--ORDER BY ParcelID
)
SELECT *
FROM RowNumCTE
WHERE RowNUm > 1
ORDER BY PropertyAddress

-- And then delete those duplicate (simply change the SELECT * TO DELETE)
WITH RowNumCTE AS(
SELECT *,
	ROW_NUMBER() OVER (
	PARTITION BY ParcelID,
				PropertyAddress,
				SalePrice,
				SaleDate,
				LegalReference
				ORDER BY
					UniqueID
					) AS RowNum
FROM Housing
--ORDER BY ParcelID
)
DELETE
FROM RowNumCTE
WHERE RowNUm > 1
--ORDER BY PropertyAddress

-- Delete unused columns
SELECT *
FROM Housing

ALTER TABLE Housing
DROP COLUMN OwnerAddress, TaxDistrict,PropertyAddress,SplitCity

ALTER TABLE Housing
DROP COLUMN SaleDate
