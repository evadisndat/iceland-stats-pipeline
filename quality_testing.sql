
------------------------------------------------------------
-- 1. Fjöldi raða
-- Gæðaprófun: Athugar hvort allar færslur hafi skilað sér í SQL.
-- Niðurstaða: Skilaði 132 röðum í báðum töflum. (11 ár x 12 mánuðir)
------------------------------------------------------------
SELECT COUNT(*) FROM unemployment_monthly;
SELECT COUNT(*) FROM buisness_registrations_monthly;

------------------------------------------------------------
-- 2. Árabil gagna
-- Gæðaprófun: Staðfestir að gögn nái yfir rétt tímabil (2013–2023).
-- Niðurstaða: Báðar töflur ná frá árinu 2013 til 2023.
------------------------------------------------------------
SELECT MIN(year), MAX(year) FROM unemployment_monthly;
SELECT MIN(year), MAX(year) FROM buisness_registrations_monthly;

------------------------------------------------------------
-- 3. Tviteknar raðir
-- Gæðaprófun: Athugar hvort fleiri en ein færsla sé til fyrir sama ár og mánuð.
-- Niðurstaða: Engar tvíteknar raðir fundust.
------------------------------------------------------------
SELECT year, month, COUNT(*) AS c
FROM unemployment_monthly
GROUP BY year, month
HAVING COUNT(*) > 1;


SELECT year, month, COUNT(*) AS c
FROM buisness_registrations_monthly
GROUP BY year, month
HAVING COUNT(*) > 1;

------------------------------------------------------------
-- 4. NULL gildi í lykildálkum
-- Gæðaprófun: Tryggir að engin gildi vanti.
-- Niðurstaða: Engin NULL gildi fundust.
------------------------------------------------------------
SELECT *
FROM unemployment_monthly
WHERE year IS NULL OR month IS NULL OR unemployed IS NULL;

SELECT *
FROM buisness_registrations_monthly
WHERE year IS NULL OR month IS NULL OR new_registrations IS NULL;


------------------------------------------------------------
-- 5. Join-prófun milli gagnasetta
-- Gæðaprófun: Staðfestir að bæði gagnasett innihaldi sömu mánuði.
-- Niðurstaða: Join skilaði 132 röðum, engir mánuðir vanta.
------------------------------------------------------------
SELECT COUNT(*)
FROM unemployment_monthly u
JOIN buisness_registrations_monthly b
  ON u.year = b.year AND u.month = b.month;

------------------------------------------------------------
-- 6. Samtölur og meðaltöl – atvinnuleysi
-- Gæðaprófun: Ber saman niðurstöður úr SQL við Python og Excel.
-- Niðurstaða:
--   Fjöldi raða: 132
--   Heildarfjöldi atvinnulausra yfir öll árin: 1.171.800
--   Meðaltal á mánuði: 8.877
------------------------------------------------------------
SELECT
  COUNT(*) AS rows,
  SUM(unemployed) AS total_unemployed,
  AVG(unemployed) AS avg_unemployed
FROM unemployment_monthly;

------------------------------------------------------------
-- 7. Samtölur og meðaltöl – nýskráningar fyrirtækja
-- Gæðaprófun: Staðfestir að gögn hafi ekki breyst milli kerfa.
-- Niðurstaða:
--   Fjöldi raða: 132
--   Heildarfjöldi nýskráninga: 31.761
--   Meðaltal á mánuði: 240
------------------------------------------------------------

SELECT
  COUNT(*) AS rows,
  SUM(new_registrations) AS total_registrations,
  AVG(new_registrations) AS avg_registrations
FROM buisness_registrations_monthly;



