@echo off
cd /d "%~dp0.."
pipx install poetry
poetry install
if errorlevel 1 exit /b
poetry config --local virtualenvs.in-project true
poetry install
if errorlevel 1 exit /b
call .venv\Scripts\activate.bat
set PYTHONPATH=.
cls
poetry run python src/demo/main.py
