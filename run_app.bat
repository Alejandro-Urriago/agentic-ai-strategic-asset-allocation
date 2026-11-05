@echo off
cd /d "%~dp0app"
"..\.venv\Scripts\streamlit.exe" run app.py
pause
