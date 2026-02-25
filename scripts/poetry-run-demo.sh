cd "$(dirname "${BASH_SOURCE[0]}")" || exit
cd ..
pipx install poetry
poetry install || exit
poetry config --local virtualenvs.in-project true

poetry install || exit
source .venv/bin/activate
export PYTHONPATH="."
clear
poetry run python src/demo/main.py