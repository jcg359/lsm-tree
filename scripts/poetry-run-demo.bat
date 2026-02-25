@echo off
cd /d "%~dp0.."
pipx install poetry

set "PIPX_BIN=%USERPROFILE%\.local\bin"

:: Check if poetry.exe actually exists there
if exist "%PIPX_BIN%\poetry.exe" (
    set "PATH=%PATH%;%PIPX_BIN%"
    echo [SUCCESS] pipx bin folder added to session PATH.
) else (
    echo [ERROR] poetry.exe not found in %PIPX_BIN%
    echo Try running 'pipx list' in a normal terminal to see where your apps are.
)

poetry install
poetry add pyreadline3

if errorlevel 1 exit /b
poetry config --local virtualenvs.in-project true
poetry install
if errorlevel 1 exit /b
call .venv\Scripts\activate.bat
set PYTHONPATH=.
cls
poetry run python src/demo/main.py
