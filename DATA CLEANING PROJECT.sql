-- DATA CLEANING

SELECT * FROM LAYOFFS; 


CREATE TABLE layoffs_staging       -- create a copy of layoffs like layoffs_staging 
Like layoffs; 

Select * from layoffs_staging;


Insert into layoffs_staging                    
Select * from layoffs; 						



												-- 1. DELETE DUPLICATE/REPEATED DATA
                                                
SELECT *,ROW_NUMBER() OVER()           
FROM LAYOFFS_STAGING;  
 
SELECT *, 
ROW_NUMBER() OVER(PARTITION BY COMPANY,LOCATION,INDUSTRY,TOTAL_LAID_OFF,
percentage_laid_off,`DATE`,STAGE,COUNTRY,FUNDS_RAISED_MILLIONS)  row_num         
FROM LAYOFFS_STAGING;  


WITH DUPLICATE_CTE AS
(SELECT *, 
ROW_NUMBER() OVER(PARTITION BY COMPANY,LOCATION,INDUSTRY,TOTAL_LAID_OFF,
percentage_laid_off,`DATE`,STAGE,COUNTRY,FUNDS_RAISED_MILLIONS)  row_num          
FROM LAYOFFS_STAGING
) 
SELECT * 
FROM DUPLICATE_CTE 
WHERE ROW_NUM>1;      
 

WITH DUPLICATE_CTE AS
(SELECT *, 
ROW_NUMBER() OVER(PARTITION BY COMPANY,LOCATION,INDUSTRY,TOTAL_LAID_OFF,
percentage_laid_off,`DATE`,STAGE,COUNTRY,FUNDS_RAISED_MILLIONS)  row_num          
FROM LAYOFFS_STAGING
)
DELETE 
FROM DUPLICATE_CTE 
WHERE ROW_NUM>1;
 
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
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


INSERT INTO layoffs_staging2
SELECT *, 
ROW_NUMBER() OVER(PARTITION BY COMPANY,LOCATION,INDUSTRY,TOTAL_LAID_OFF,
percentage_laid_off,`DATE`,STAGE,COUNTRY,FUNDS_RAISED_MILLIONS)  row_num          
FROM LAYOFFS_STAGING;  

select * from layoffs_staging2;

select *
FROM  layoffs_staging2 
WHERE ROW_NUM>1;

DELETE
FROM  layoffs_staging2 
WHERE ROW_NUM>1;
 



															-- 2. STANDARDIZING THE DATA
                                                             
SELECT * FROM layoffs_staging2;

-- A. company 
	SELECT distinct COMPANY 
    FROM layoffs_staging2;   

	SELECT COMPANY,TRIM(COMPANY) 
    FROM layoffs_staging2;

	UPDATE layoffs_staging2
	SET COMPANY = TRIM(COMPANY);

	SELECT distinct COMPANY 
    FROM layoffs_staging2;


-- B. location
	SELECT distinct location 
    FROM layoffs_staging2 
    order by 1;

	SELECT LOCATION,TRIM(LOCATION) 
    FROM layoffs_staging2;

	UPDATE layoffs_staging2
	SET LOCATION = TRIM(LOCATION);

	UPDATE layoffs_staging2
	SET LOCATION = 'Düsseldorf'
	WHERE LOCATION = 'DÃ¼sseldorf';
	 
	UPDATE layoffs_staging2
	SET LOCATION = 'Florianópolis'
	WHERE LOCATION = 'FlorianÃ³polis'; 

	UPDATE layoffs_staging2
	SET LOCATION = 'Malmö'
	WHERE LOCATION = 'MalmÃ¶';

	SELECT distinct location FROM layoffs_staging2 order by 1;


-- C. INDUSTRY
	SELECT distinct INDUSTRY 
    FROM layoffs_staging2 
    order by 1;
    
    SELECT * 
    FROM layoffs_staging2 
    WHERE INDUSTRY LIKE 'CRYPTO%';
    
    UPDATE layoffs_staging2
    SET INDUSTRY ='Crypto'
    WHERE INDUSTRY LIKE 'CRYPTO%';

	SELECT distinct INDUSTRY FROM layoffs_staging2 order by 1;


-- D. COUNTRY
 	SELECT distinct COUNTRY 
    FROM layoffs_staging2 
    order by 1;
    
    SELECT COUNTRY 
    FROM layoffs_staging2 
    WHERE COUNTRY LIKE 'United States_';
    
    SELECT DISTINCT COUNTRY,TRIM(TRAILING '.' FROM COUNTRY) 
    FROM layoffs_staging2 
    ORDER BY 1;

	UPDATE layoffs_staging2
    SET COUNTRY = TRIM(TRAILING '.' FROM COUNTRY) 
    WHERE COUNTRY LIKE 'UNITED STATES%';

	SELECT DISTINCT COUNTRY FROM layoffs_staging2 ORDER BY 1;


-- E. DATE
	SELECT `DATE` FROM layoffs_staging2;
 
	SELECT `DATE`, 
    STR_TO_DATE(`date`,'%m/%d/%Y')                
    FROM layoffs_staging2;

	UPDATE layoffs_staging2
    SET `DATE` = STR_TO_DATE(`date`,'%m/%d/%Y');
		
    ALTER TABLE layoffs_staging2
    MODIFY COLUMN `date`  DATE; 
    
    
    
    
    
                                                          -- 3. DEALING NULL/BLANK VALUES AND GENERATE DATA FOR IT

SELECT * 
FROM layoffs_staging2;


SELECT * 
FROM layoffs_staging2
where location =''
or location is NULL;

SELECT * 
FROM layoffs_staging2
where industry =''
or industry is NULL;
 
SELECT * 
FROM layoffs_staging2
where company ='airbnb';  
  
  
SELECT T1.COMPANY,T1.INDUSTRY,T2.INDUSTRY 
FROM layoffs_staging2 T1
JOIN  layoffs_staging2 T2
	ON T1.COMPANY=T2.COMPANY
    WHERE (T1.INDUSTRY IS NULL OR T1.INDUSTRY='')
    AND T2.INDUSTRY IS NOT NULL;
 
 
 UPDATE layoffs_staging2 T1
 JOIN  layoffs_staging2 T2
 ON T1.COMPANY = T2.COMPANY
    SET  T1.INDUSTRY=T2.INDUSTRY
    WHERE T1.INDUSTRY=''
    AND T2.INDUSTRY IS NOT NULL;

UPDATE layoffs_staging2
SET INDUSTRY = NULL
WHERE INDUSTRY='';
    
SELECT * 
FROM layoffs_staging2
where industry =''
or industry is NULL;     

 UPDATE layoffs_staging2 T1
 JOIN layoffs_staging2 T2
 ON T1.COMPANY = T2.COMPANY
	SET T1.INDUSTRY = T2.INDUSTRY
		WHERE T1.INDUSTRY IS NULL
        AND T2.INDUSTRY IS NOT NULL;
        
SELECT * 
FROM layoffs_staging2
where industry =''
or industry is NULL;   

SELECT *
FROM layoffs_staging2
WHERE COMPANY LIKE 'BALLY%';


select * 
from  layoffs_staging2
where total_laid_off is null or total_laid_off =''
and percentage_laid_off is null or percentage_laid_off ='';

 
 
                                                           -- 4. Remove any Columns or Rows that aren't necessary

select *
from layoffs_staging2 
where percentage_laid_off = '';

select *
from layoffs_staging2 
where total_laid_off = ''; 


select *
from layoffs_staging2 
where total_laid_off is null 
and percentage_laid_off is null;    


delete
from layoffs_staging2 
where total_laid_off is null 
and percentage_laid_off is null;   
    

select * 
from layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN ROW_NUM;
    
SELECT *
FROM LAYOFFS_STAGING2;
