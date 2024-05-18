-- data cleaning

#working with dataset world_layoffs from Alexander (Alex The Analyst) | 18.05.2024

# 1. Import dataset as raw data

select *
from layoffs;

# 2. Remove duplicates (if there are any)
# 3. Standardize the data
# 4. Null Values (missings/ blank values)
# 5. Remove not relevent columns (not from the original dataset)


CREATE TABLE layoffs_staging
LIKE layoffs;						#not working on original dataset

SELECT *
FROM layoffs_staging;				#only empty columns

INSERT layoffs_staging				#fill them with the data from original dataset
Select *
FROM layoffs;

# 2. Remove duplicates (if there are any)

SELECT *, 
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`)  AS row_numb #define duplicate - what must be identical to be a duplicate? // date is a keyword - don't forget ``
FROM layoffs_staging;   #greater than 1 is an issue

# we want that it can be selected, if it is greater than 1

WITH duplicate_CTE AS
(
SELECT *, 
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`)  AS row_numb 
FROM layoffs_staging  
)
SELECT *
FROM duplicate_CTE
WHERE row_numb > 1;

SELECT *
FROM layoffs_staging
WHERE company = 'Oda';

#Oda is not a duplicate - partition by must be over all rows

WITH duplicate_CTE2 AS
(
SELECT *, 
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, stage, country, funds_raised_millions, `date`)  AS row_numb 
FROM layoffs_staging  
)
SELECT *
FROM duplicate_CTE2
WHERE row_numb > 1;

SELECT *
FROM layoffs_staging
WHERE company = 'Cazoo';

# only remove one of those					# not possible by DELETE with CTE, it is like an update statement

# could put it into a staging database - temporal storage area during ETL (extract, transform and load) process
# filter the row_numbs & delete it

#table "layoffs_staging" - copy to clipboard - create statement - insert

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_numb` INT											#new colomn
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT *, 
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, stage, country, funds_raised_millions, `date`)  AS row_numb 
FROM layoffs_staging ;

SELECT *
FROM layoffs_staging2
WHERE row_numb > 1;

DELETE
FROM layoffs_staging2
WHERE row_numb > 1;

SELECT *
FROM layoffs_staging2
WHERE row_numb > 1;

-- standardizing data

SELECT company, (TRIM(company))   # delete white space
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = (TRIM(company));

SELECT *
FROM layoffs_staging2;

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;   #first column

# crypto, crypto currency & cyptocurrency should be one industry

SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';           # or 'Crypto' OR 'Crypto Currency' OR 'CryptoCurrency';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;

SELECT DISTINCT location
FROM layoffs_staging2
ORDER BY 1;

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;

#issue with United States.

SELECT *
FROM layoffs_staging2
WHERE country LIKE 'United States%';

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)  # coming from the right
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

SELECT *
FROM layoffs_staging2;

# date is a TEXT field - not good for time series - change to date

SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')    #capital Y!
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

SELECT *
FROM layoffs_staging2;

#change it to date-column
#not on raw data!

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

SELECT *
FROM layoffs_staging2;

#NULL/blank values

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL;  # IS NULL, not =


SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;   #if both NULL mabe useless

# Not sure if missings are blank or NULL - set it to NULL

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';

# Like Airbnb has travel - we know the industry - just insert into the missings

SELECT *
 FROM layoffs_staging2 t1
 JOIN layoffs_staging2 t2
	ON t1.company = t2.company
    AND t1.location = t2.location   # maybe Airbnb in another location is not traveling
WHERE (t1.industry IS NULL OR  t1.industry  ='') 
AND t2.industry IS NOT NULL;


SELECT t1.industry, t2.industry
 FROM layoffs_staging2 t1
 JOIN layoffs_staging2 t2
	ON t1.company = t2.company
    AND t1.location = t2.location   
WHERE t1.industry IS NULL 
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.company = t2.company
    AND t1.location = t2.location   
AND t1.industry IS NULL 
AND t2.industry IS NOT NULL;

SELECT *
FROM layoffs_staging2
WHERE company LIKE 'Ball%';   #no other row with Bally's

# if total_laid_off and percentage_laid_off is NULL - not very useful - delet the rows

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_numb;

 









