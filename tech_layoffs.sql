-- DATA CLEANING
# 1 - Remove Duplicates
# 2 - Standardize Data
# 3 - Null/Blank Values
# 4 - Remove Columns

SELECT *
FROM layoffs;


-- STAGING TABLE
# create staging table
CREATE TABLE layoffs_1
LIKE layoffs;

# insert data (from layoffs table) into staging table
INSERT layoffs_1
SELECT *
FROM layoffs;

SELECT *
FROM layoffs_1;


-- 1 REMOVE DUPLICATES
# use row_num column to identify any duplicate rows in staging table
# row_num is a temporary view column, not a permanent column
SELECT *, ROW_NUMBER() OVER(PARTITION BY company, location, total_laid_off, `date`, percentage_laid_off, industry, 'source', stage, funds_raised, country, date_added) AS row_num
FROM layoffs_1;

# create second staging table and insert row_num col into the table
CREATE TABLE `layoffs_2` (
  `company` text,
  `location` text,
  `total_laid_off` double DEFAULT NULL,
  `date` text,
  `percentage_laid_off` text,
  `industry` text,
  `source` text,
  `stage` text,
  `funds_raised` text,
  `country` text,
  `date_added` text,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO layoffs_2
SELECT *, ROW_NUMBER() OVER(PARTITION BY company, location, total_laid_off, `date`, percentage_laid_off, industry, 'source', stage, funds_raised, country, date_added) AS row_num 
FROM layoffs_1;

# delete the duplicate rows (where row_num > 1) from second staging table
DELETE
FROM layoffs_2
WHERE row_num > 1;

# check that all duplicate rows have been removed successfully
SELECT *
FROM layoffs_2;


-- 2 STANDARDIZE DATA 
# look through columns, see if any outstanding issues 

# trim whitespaces from company column values
SELECT DISTINCT company, TRIM(company)
FROM layoffs_2;

UPDATE layoffs_2
SET company = TRIM(company);


# add whitespace following ',' in location column values
SELECT DISTINCT location
FROM layoffs_2
WHERE location LIKE '%,%';

UPDATE layoffs_2
SET location = CONCAT(SUBSTRING(location, 1, LOCATE(',', location)), ' ', SUBSTRING(location, LOCATE(',', location)+1));


# change date column values from text to date format, then convert to DATE data type 
SELECT `date`, STR_TO_DATE(`date`, "%m/%d/%Y")
FROM layoffs_2;

UPDATE layoffs_2
SET `date` = STR_TO_DATE(`date`, "%m/%d/%Y");

ALTER TABLE layoffs_2
MODIFY COLUMN `date` DATE;


# change date_added column values from text to date format, then convert to DATE data type 
SELECT date_added, STR_TO_DATE(date_added, "%m/%d/%Y")
FROM layoffs_2;

UPDATE layoffs_2
SET date_added = STR_TO_DATE(date_added, "%m/%d/%Y");

ALTER TABLE layoffs_2
MODIFY COLUMN date_added DATE;


-- 3 NULL/BLANK VALUES
# funds_raised, industry, stage, percentage_laid_off columns have some null/blank values
# cannot populate null/blank values
SELECT *
FROM layoffs_2
WHERE industry IS NULL 
OR industry = '';


-- 4 REMOVE UNNECESSARY COLUMNS
# remove row_num column, done using it to identify duplicate rows
ALTER TABLE layoffs_2
DROP COLUMN row_num;

SELECT *
FROM layoffs_2;


-- EXPLORATORY DATA ANALYSIS

-- 1. total_laid_off (global) per year
#1a sum of total_laid_off every year
SELECT SUM(total_laid_off), YEAR(`date`)
FROM layoffs_2
GROUP BY YEAR(`date`)
ORDER BY YEAR(`date`);

#1b ultimate sum of total_laid_off
SELECT SUM(total_laid_off)
FROM layoffs_2;

-- 2. sum of total_laid_off per country
#2a sum of total_laid_off per country, top 5
SELECT SUM(total_laid_off), country
FROM layoffs_2
GROUP BY country
ORDER BY SUM(total_laid_off) DESC;

#2b sum of total_laid_off per country, bottom 5
SELECT SUM(total_laid_off), country
FROM layoffs_2
GROUP BY country
ORDER BY SUM(total_laid_off);

-- 3. sum of total_laid_off per industry
#3a sum of total_laid_off per industry, top 5
SELECT SUM(total_laid_off), industry
FROM layoffs_2
GROUP BY industry
ORDER BY SUM(total_laid_off) DESC;

#3b sum of total_laid_off per industry, bottom 5
SELECT SUM(total_laid_off), industry
FROM layoffs_2
GROUP BY industry
ORDER BY SUM(total_laid_off);

-- 4. sum of total_laid_off per company
#4a sum of total_laid_off per company, top 5
SELECT SUM(total_laid_off), company
FROM layoffs_2
GROUP BY company
ORDER BY SUM(total_laid_off) DESC;

#4b sum of total_laid_off per company, bottom 5
SELECT SUM(total_laid_off), company
FROM layoffs_2
GROUP BY company
ORDER BY SUM(total_laid_off);

-- 5. percentage_laid_off = 1, total_laid_off per company
SELECT total_laid_off, company, YEAR(`date`)
FROM layoffs_2
WHERE percentage_laid_off = 1
ORDER BY total_laid_off DESC;

-- 6. company with MAX total_laid_off
SELECT *
FROM layoffs_2
WHERE total_laid_off = 
	(
		SELECT MAX(total_laid_off) 
        FROM layoffs_2
	);

-- 7. company with MIN total_laid_off
SELECT *
FROM layoffs_2
WHERE total_laid_off = 
	(
		SELECT MIN(total_laid_off) 
        FROM layoffs_2
	);

-- 8. company with MAX funds_raised
SELECT *
FROM layoffs_2
WHERE funds_raised = 
	(
		SELECT MAX(funds_raised) 
        FROM layoffs_2
	);

-- 9. company with MIN funds_raised, where funds_raised is not blank
SELECT *
FROM layoffs_2
WHERE funds_raised = 
	(
		SELECT MIN(funds_raised) 
        FROM layoffs_2
		WHERE funds_raised != ''
	);

-- 10. top sum of total_laid_off for industries in USA
SELECT industry, SUM(total_laid_off)
FROM layoffs_2
WHERE country = 'United States'
GROUP BY industry
ORDER BY SUM(total_laid_off) DESC;

-- 11. top sum of total_laid_off for companies in USA
SELECT company, SUM(total_laid_off)
FROM layoffs_2
WHERE country = 'United States'
GROUP BY company
ORDER BY SUM(total_laid_off) DESC;

-- 12. top sum of total_laid_off for countries in 2023
SELECT country, SUM(total_laid_off)
FROM layoffs_2
WHERE YEAR(`date`) = '2023'
GROUP BY country
ORDER BY SUM(total_laid_off) DESC;

-- 13. top sum of total_laid_off for industries in 2023
SELECT industry, SUM(total_laid_off)
FROM layoffs_2
WHERE YEAR(`date`) = '2023' AND country = 'United States'
GROUP BY industry
ORDER BY SUM(total_laid_off) DESC;

-- 14. top sum of total_laid_off for companies in 2023
SELECT company, SUM(total_laid_off), industry
FROM layoffs_2
WHERE YEAR(`date`) = '2023' AND country = 'United States'
GROUP BY company, industry
ORDER BY SUM(total_laid_off) DESC;

