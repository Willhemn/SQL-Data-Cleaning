SELECT *
FROM world_layoffs_2023;
-- Main Steps for Data Cleaning:
-- 1. Remove Duplicates
-- 2. Standarize the Data
-- 3. Verify and work on Null values
-- 4. Analyze and remove any unnecessary colums

-- To start, as a best practice, 
-- I'll clone our base data set to a "Staging" table be sure we have a backup of the raw data:

CREATE TABLE world_layoffs_staging
LIKE world_layoffs_2023;

INSERT INTO world_layoffs_staging
SELECT *
FROM world_layoffs_2023;

-- Step No.1, Identify and Remove Duplicates

-- We identify if there are repeated rows by adding row numbers 
-- partitioned by ALL the columns and looking, with a CTE, for values greater than one:
WITH row_record_CTE AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, 
country, funds_raised_millions) 
AS row_record
FROM world_layoffs_staging
)
SELECT *
FROM row_record_CTE
WHERE row_record > 1;

-- As it is not possible (in MySQL) to DELETE from a CTE, we create a new table with our row_record column:
CREATE TABLE `world_layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_record` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO world_layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, 
country, funds_raised_millions) 
AS row_record
FROM world_layoffs_staging;

-- Verify and then DELETE the duplicate rows:
SELECT *
FROM world_layoffs_staging2
WHERE row_record > 1;

DELETE
FROM world_layoffs_staging2
WHERE row_record > 1;

-- Step No.2 - Standarizing the Data

-- Let's start by TRIMMING the string type columns
UPDATE world_layoffs_staging2
SET company = TRIM(company);

-- Standarize the industry type columns, identifying erros on the strings
SELECT DISTINCT industry
FROM world_layoffs_staging2
ORDER BY industry;

-- Crypto is wrongly written in some cases, we find: CryptoCurrency, Crypto Currency, let's standarize it
UPDATE world_layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- We do the same with different columns:
SELECT DISTINCT country
FROM world_layoffs_staging2
ORDER BY 1;

-- Some United States records have dots at the end, let's remove then:
UPDATE world_layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

-- Our date column is formatted as a String, we will proceed to convert it into a date format
SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y') AS formated_date
FROM world_layoffs_staging2;

UPDATE world_layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE world_layoffs_staging2
MODIFY COLUMN `date` DATE;

-- Step No.3 - Verify and work on Null values
SELECT *
FROM world_layoffs_staging2
WHERE industry IS NULL 
OR industry = '';

-- Let's look if another company has already populated data on the industry and update it:
UPDATE world_layoffs_staging2
SET industry = NULL
WHERE industry = '';

UPDATE world_layoffs_staging2 AS t1
JOIN world_layoffs_staging2 AS t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- In cases like total_laid_off, percentage_laid_off and funds_raised are impossible 
-- to populate as we are lacking information, further decisions for these should be discussed.
-- Let's suppose the team decided to delete rows that have NULLS on these fields.

SELECT*
FROM world_layoffs_staging2
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL;

DELETE FROM world_layoffs_staging2
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL;

-- To the final, Step No.4 - Analyze and remove any unnecessary colums

ALTER TABLE world_layoffs_staging2
DROP COLUMN row_record;

-- And with this we have a clean data set ready for Analysis.












