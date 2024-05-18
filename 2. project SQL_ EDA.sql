-- exploratory data analysis

#building up on project data cleaning

#working with dataset world_layoffs from Alexander (Alex The Analyst) | 18.05.2024

SELECT *
FROM layoffs_staging2;

#max values for laid offs and percentage of laid offs (12000 & 100%)

SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM layoffs_staging2;										#1 is 100%

# how much funds raised in descending order (Top 3: 2400, 1800, 1700)

SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

# which company laid off how many people in sum (Top 3: Amazon, Google, Meta)

SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;    #2nd columnd (SUM)

# when did it start - when end (2020-03-11 (around COVID 19) till 2023-03-06 - duration around 3 years)

SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging2;

# wich industry had the most laid offs (Top 3: Consumer, Retail and other)

SELECT industry, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;

# which countries had the most laid offs? (Top 3: US, India and Netherlands)

SELECT country, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;

# which days where the most laid offs? (Top 3: 2023-03-06, 2023-03-03 and 2023-03-02)

SELECT `date`, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY `date`
ORDER BY 1 DESC;

# which years were the most laid offs? (TOP 3:  2022, 2023, 2020) to note: 2023 only three month

SELECT YEAR (`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY 2 DESC;

#laid offs sorted by stage (Top 3: Post-IPO, Unknown & Acquired)

SELECT stage, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC;

# avg percentage of laid offs... not really relevant, we don't know the absolute number of employees

SELECT company, AVG(percentage_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

# rolling sum of laid offs (sum 383159 laid offs; 2020: 80998, 2021: 96821, 2022: 257482, 2023: 383159)

SELECT SUBSTRING(`date`, 1,7) AS `month`, SUM(total_laid_off)
FROM layoffs_staging2
WHERE SUBSTRING(`date`, 1,7) IS NOT NULL
GROUP BY `month`
ORDER BY 1 ASC;

WITH Rolling_Total AS 
(
SELECT SUBSTRING(`date`, 1,7) AS `month`, SUM(total_laid_off) AS total_off
FROM layoffs_staging2
WHERE SUBSTRING(`date`, 1,7) IS NOT NULL
GROUP BY `month`
ORDER BY 1 ASC
)
SELECT `month`, total_off
,SUM(total_off) OVER(ORDER BY `month`) as rolling_total
FROM Rolling_total;

# who laid off the most people per years? (2020 Top3: Uber, Booking.com, Groupon; 2021: Bytedance, Katerra, Zillow; 2022: Meta, Ammazon, Cisco; 2023: Google, Microsoft, Ericsson)

SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
ORDER BY 3 DESC;

WITH Company_Year (company, years, total_laid_off) AS
(
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`) 
), Company_Year_RANK AS
(SELECT *, 
DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS Ranking
FROM Company_Year
WHERE years IS NOT NULL)
SELECT *
FROM Company_Year_RANK
WHERE Ranking <= 5;













