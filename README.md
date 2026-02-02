## First time setup:

1. Búa til virtual environment í rótinni á projectinu:
`python -m venv .venv`

2. Activate-a virtual environment:

macOS/Linux: `source .venv/bin/activate`
Windows: `.\.venv\Scripts\Activate.ps1`

3. Install dependencies:
`python -m pip install -r requirements.txt`

4. Búa til .env skrá í rótinni á projectinu með því sem sent var á Discord:

5. Tengjast VScodeto Azure. Install-a t.d. `SQL Server (mssql)` extension-ið í VScode.

6. Opna command pallette (ctrl+shift+p) og velja `MS SQL: Connect` og fylla inn upplýsingar úr .env skránni.

7. Vera viss um að réttur SQL server sé installaður (ODBC Driver for SQL Server):
https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver17

8. Keyra pipeline_atvinnuleysi.py og pipeline_fyrirtaeki.py til að hlaða gögnum inn í gagnagrunninn.

<!-- What I used to connect VScodeto Azure is MS SQL vs code extension - install,
then do ctrl+shift+p (or whatever windows wants to go into the command pallette) fill in .env information sent on discord. After uploading you always need to refresh to see updated changes. 
Logic of updating the database is handled in azure_db
After connecting you can browse tables in vscode,  if we prefer to use something else we can change this easily
NOTE: the extension only connects VScode - Azure for looking at things, we will always need to use the pipeline to update first before looking at it. -->


Verkefnið snýst um gagnaforritun þar sem gögn eru sótt af Hagstofu Íslands, hreinsuð og unnin í Python og vistuð í SQL gagnagrunni. Tvö aðskilin gagnasett eru notuð í verkefninu. Annað gagnasettið lýsir atvinnuleysi á Íslandi og er vistað í SQL töflunni unemployment_monthly, en hitt gagnasettið lýsir nýskráningum fyrirtækja og er vistað í töflunni business_registrations_monthly. Gögnin eru sótt sem CSV skrár af Hagstofu Íslands og allar lagfæringar, síanir og umbreytingar eru framkvæmdar í Python samkvæmt kröfum verkefnisins.

Python forritið les CSV skrárnar með pandas, staðlar dálkanöfn, vinnur ár og mánuð úr dagsetningum og síar gögnin í tímabilið 2013 til 2023. Aðeins þær breytur sem nýtast til að segja söguna eru sóttar og niðurstaðan er skrifuð í Azure SQL gagnagrunn. Fyrir hvort gagnasett er stofnuð sér SQL tafla sem er yfirskrifuð við hverja keyrslu pipeline

Gæðaprófanir voru framkvæmdar í SQL til að staðfesta að gögnin hafi flust rétt í gegnum öll skref vinnslunnar. Athugaður var fjöldi raða í hvorri töflu, staðfest var að tímabilið næði frá 2013 til 2023, tryggt var að engar tvíteknar raðir væru til staðar og að engin NULL gildi kæmu fyrir í lykildálkum. Að auki var framkvæmd join-prófun til að staðfesta að báðar töflur innihéldu sömu mánuði og að gögnin pössuðu saman. Heildartölur voru einnig bornar saman við tölur beint af Hagstofu Íslands til að tryggja réttleika gagna.

Dataset 1: https://www.hagstofa.is/talnaefni/samfelag/vinnumarkadur/vinnumarkadsrannsokn/ 
valið er "Vinnuafl, starfandi, atvinnulausir og vinnustundir - Mæling - Mánaðarlegar tölur 2003-2025" 
Allar breytur eru valdar og því downloadað sem semíkommuskiptu csv með töfluheiti.
Pipeline sér um að filtera eftir atvinnulausir alls og árunum 2013 - 2023

Dataset 2:
https://www.hagstofa.is/talnaefni/atvinnuvegir/fyrirtaeki/fyrirtaeki/
Valið er allir mánuðir  "alls" í atvinnugreinar, "alls" í rekstrarform og allt í breytur. Ástæðan fyrir því að sía þurfti örlítið í atvinnugreinum og rekstrarformum er að hagstofan býður ekki upp á að sækja gögn sem eru meir en 100.000 línur or því þurfti að þrengja leitina áður en gögn voru sótt. 
Niðurstöður voru sóttar sem semíkommuskipt CSV og pipeline sér um að filtera gögn sem við þurfum