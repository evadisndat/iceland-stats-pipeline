------------------------------------------------------------
-- 1. Fjöldi raða
-- Gæðaprófun: Athugar hvort allar færslur (11 ár * 12 mánuðir = 132) hafi skilað sér.
-- Niðurstaða: Skilaði 132 röðum í báðum töflum. (11 ár x 12 mánuðir)
------------------------------------------------------------
SELECT COUNT(*) FROM unemployment_monthly;
SELECT COUNT(*) FROM business_registrations_monthly;

------------------------------------------------------------
-- 2. Árabil gagna
-- Gæðaprófun: Staðfestir að gögn nái yfir rétt tímabil (2013-01-01 til 2023-12-01).
-- Niðurstaða: Báðar töflur ná frá árinu 2013 til 2023.
------------------------------------------------------------
SELECT MIN(mánuður), MAX(mánuður) FROM unemployment_monthly;
SELECT MIN(mánuður), MAX(mánuður) FROM business_registrations_monthly;

------------------------------------------------------------
-- 3. Tviteknar raðir
-- Gæðaprófun: Athugar hvort fleiri en ein færsla sé til fyrir sama mánuð.
-- Niðurstaða: Engar tvíteknar raðir fundust.
------------------------------------------------------------
SELECT mánuður, COUNT(*) 
FROM unemployment_monthly 
GROUP BY mánuður 
HAVING COUNT(*) > 1;

SELECT mánuður, COUNT(*) 
FROM business_registrations_monthly 
GROUP BY mánuður 
HAVING COUNT(*) > 1;

------------------------------------------------------------
-- 4. Gagnatög (Data Types)
-- Gæðaprófun: Staðfestir að mánuður sé DATE og tölur séu INT.
-- Það stemmir og sýnir að gagnagrunnurinn sé rétt uppsettur
------------------------------------------------------------
SELECT COLUMN_NAME, DATA_TYPE 
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_NAME IN ('unemployment_monthly', 'business_registrations_monthly');

------------------------------------------------------------
-- 5. Join-prófun milli gagnasetta
-- Gæðaprófun: Staðfestir að hægt sé að tengja gagnasettin saman á mánuði.
-- Niðurstaða: Join skilaði 132 röðum, engir mánuðir vanta.
------------------------------------------------------------
SELECT COUNT(*)
FROM unemployment_monthly u
JOIN business_registrations_monthly b
  ON u.mánuður = b.mánuður;

------------------------------------------------------------
-- 6. Samtölur og meðaltöl – Atvinnuleysi
-- Niðurstaða:
--   Fjöldi raða: 132
--   Heildarfjöldi atvinnulausra yfir öll árin: 1.171.800
--   Meðaltal á mánuði: 8.877
------------------------------------------------------------
SELECT
  COUNT(*),
  SUM(atvinnulausir),
  AVG(atvinnulausir)
FROM unemployment_monthly;

------------------------------------------------------------
-- 7. Samtölur og meðaltöl – Nýskráningar
-- Niðurstaða:
--   Fjöldi raða: 132
--   Heildarfjöldi nýskráninga: 31.761
--   Meðaltal á mánuði: 240
------------------------------------------------------------
SELECT
  COUNT(*),
  SUM(nýskráningar),
  AVG(nýskráningar) 
FROM business_registrations_monthly;

------------------------------------------------------------
-- 8. NULL gildi í lykildálkum
-- Gæðaprófun: Tryggir að engin gildi vanti.
-- Niðurstaða: Engin NULL gildi fundust.
------------------------------------------------------------
SELECT *
FROM unemployment_monthly
WHERE mánuður IS NULL OR atvinnulausir IS NULL;

SELECT *
FROM business_registrations_monthly
WHERE mánuður IS NULL OR nýskráningar IS NULL;