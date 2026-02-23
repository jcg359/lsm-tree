cd "$(dirname "${BASH_SOURCE[0]}")" || exit
poetry install || exit
export PYTHONPATH="."
clear
poetry run python src/demo/main.py