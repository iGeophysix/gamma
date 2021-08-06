Setup
1. Go to Techlog's Python 3 root folder (e.g. C:\Program Files\Schlumberger\Techlog 2021.1 (r3532619)\Python36_x64) in Admin PowerShell
2. Run .\python.exe -m pip install -r requirements.txt
3. Import techlog_sync_project.py to Techlog
4. Set GAMMA_PROJECT_PATH script parameter to the root folder of project Gamma
5. Run the script

Warning! All data in Techlog project may be lost.

Limitations
In Techlog 2018.1 the script can only be run once during a Techlog session.
