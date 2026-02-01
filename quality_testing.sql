
-- fjöldi raða = 132 í báðum 
SELECT COUNT(*) FROM unemployment_monthly;
SELECT COUNT(*) FROM business_registrations_monthly;


--árabil frá 2013-2023 = check
SELECT MIN(year), MAX(year) FROM unemployment_monthly;
SELECT MIN(year), MAX(year) FROM business_registrations_monthly;


-- engar tviteknar raðir = 0 rows check
SELECT year, month, COUNT(*) AS c
FROM unemployment_monthly
GROUP BY year, month
HAVING COUNT(*) > 1;


SELECT year, month, COUNT(*) AS c
FROM business_registrations_monthly
GROUP BY year, month
HAVING COUNT(*) > 1;

-- engin NULL gildi í lykildálkum = check
SELECT *
FROM unemployment_monthly
WHERE year IS NULL OR month IS NULL OR unemployed IS NULL;

SELECT *
FROM business_registrations_monthly
WHERE year IS NULL OR month IS NULL OR new_registrations IS NULL;

--join test a að skila 132 annars vantar mánuð = check
SELECT COUNT(*)
FROM unemployment_monthly u
JOIN business_registrations_monthly b
  ON u.year = b.year AND u.month = b.month;
