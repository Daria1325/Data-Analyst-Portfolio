-- Create tables and import data from csv files

DROP TABLE IF EXISTS nashville_housing_data;
CREATE TABLE nashville_housing_data
(
	UniqueID text,
	ParcelID text,
	LandUse text,
	PropertyAddress text,
	SaleDate text,
	SalePrice text,
	LegalReference text,
	SoldAsVacant text,
	OwnerName text,
	OwnerAddress text,
	Acreage	text,
	TaxDistrict text,
	LandValue text,
	BuildingValue text,
	TotalValue text,
	YearBuilt text,
	Bedrooms text,
	FullBath text,
	HalfBath text
)
COPY nashville_housing_data FROM 'D:\Work\portfolio\Data cleaning\Nashville Housing Data for Data Cleaning.csv' DELIMITER ';' HEADER CSV;


-- Standardize Date Format

SELECT saledate, TO_DATE(saledate, 'Month DD, YYYY') FROM nashville_housing_data

	UPDATE nashville_housing_data
	SET saledate = TO_DATE(saledate, 'Month DD, YYYY')


-- Populate Property Address data

SELECT 
	a.parcelid,
	a.propertyaddress,
	b.parcelid,
	b.propertyaddress,
	COALESCE(a.propertyaddress, b.propertyaddress)
FROM nashville_housing_data AS a
INNER JOIN nashville_housing_data AS b
ON a.parcelid = b.parcelid AND a.uniqueid <> b.uniqueid
WHERE a.propertyaddress IS NULL

UPDATE nashville_housing_data
SET propertyaddress = COALESCE(a.propertyaddress, b.propertyaddress)
FROM nashville_housing_data AS a
INNER JOIN nashville_housing_data AS b
ON a.parcelid = b.parcelid AND a.uniqueid <> b.uniqueid
WHERE a.propertyaddress IS NULL


-- Breaking out Address into Individual Columns (Address, City, State)

SELECT propertyaddress 
FROM nashville_housing_data

SELECT 
	propertyaddress,
	SPLIT_PART(propertyaddress, ',', 1),
	SUBSTRING(propertyaddress FROM POSITION(',' IN propertyaddress) + 1)
FROM nashville_housing_data

ALTER TABLE nashville_housing_data
ADD property_split_address text;

UPDATE nashville_housing_data
SET property_split_address = SPLIT_PART(propertyaddress, ',', 1);

ALTER TABLE nashville_housing_data
ADD property_split_city text;

UPDATE nashville_housing_data
SET property_split_city = SUBSTRING(propertyaddress FROM POSITION(',' IN propertyaddress) + 1);



SELECT
	owneraddress,
  SPLIT_PART(REPLACE(owneraddress, ',', '.'), '.', 1) AS part1,
  SPLIT_PART(REPLACE(owneraddress, ',', '.'), '.', 2) AS part2,
  SPLIT_PART(REPLACE(owneraddress, ',', '.'), '.', 3) AS part3
FROM nashville_housing_data

ALTER TABLE nashville_housing_data
ADD owner_split_address text;

UPDATE nashville_housing_data
SET owner_split_address = SPLIT_PART(REPLACE(owneraddress, ',', '.'), '.', 1);

ALTER TABLE nashville_housing_data
ADD owner_split_city text;

UPDATE nashville_housing_data
SET owner_split_city = SPLIT_PART(REPLACE(owneraddress, ',', '.'), '.', 2);

ALTER TABLE nashville_housing_data
ADD owner_split_state text;

UPDATE nashville_housing_data
SET owner_split_state = SPLIT_PART(REPLACE(owneraddress, ',', '.'), '.', 3);



-- Change Y and N to Yes and No in "Sold as Vacant" field

SELECT DISTINCT(SoldAsVacant), COUNT(SoldAsVacant)
FROM nashville_housing_data
Group by SoldAsVacant
order by 2

SELECT 
	SoldAsVacant,
	CASE WHEN SoldAsVacant='Y' THEN 'Yes'
		WHEN SoldAsVacant='N' THEN 'No'
		ELSE SoldAsVacant
	END
FROM nashville_housing_data


UPDATE nashville_housing_data
SET SoldAsVacant = CASE WHEN SoldAsVacant='Y' THEN 'Yes'
						WHEN SoldAsVacant='N' THEN 'No'
						ELSE SoldAsVacant
					END


-- Remove Duplicates

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
					) row_num

FROM nashville_housing_data
)
Select *
From RowNumCTE
Where row_num > 1
Order by PropertyAddress







