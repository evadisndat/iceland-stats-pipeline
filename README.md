<<<<<<< HEAD
First time setup;
python -m venv .venv
anytime you wanna work on venv;
source .venv/bin/activate  on macOS/Linux
.\.venv\Scripts\Activate.ps1 on Windows
install dependencies:
python -m pip install -r requirements.txt






What I used to connect VScodeto Azure is MS SQL vs code extension - install, 
then do ctrl+shift+p (or whatever windows wants to go into the command pallette) fill in .env information sent on discord. After uploading you always need to refresh to see updated changes. 
Logic of updating the database is handled in azure_db
After connecting you can browse tables in vscode,  if we prefer to use something else we can change this easily
NOTE: the extension only connects VScode - Azure for looking at things, we will always need to use the pipeline to update first
before looking at it.
=======
# iceland-stats-pipeline
Business intelligence project using Icelandic public statistics.
>>>>>>> fb6d378b5687a8a64d978139a847c04b719f09b2
