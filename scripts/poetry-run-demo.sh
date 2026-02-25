cd "$(dirname "${BASH_SOURCE[0]}")" || exit
cd ..
source .venv/bin/activate
pip install poetry
poetry config --local virtualenvs.in-project true

poetry install || exit
export PYTHONPATH="."
clear
poetry run python src/demo/main.py