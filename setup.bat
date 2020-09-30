:: Ensure that `python.exe` is installed and accesible via PATH (test with`echo %PATH%`)

:: Create Virtual environment called `.venv`
python -m venv .venv

:: Activate Virtual environment
.venv\Scripts\activate.bat

:: Install requirements
pip install -r requirements.txt