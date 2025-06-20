import os


DATA_FOLDER = os.environ.get("DATA_DIR_CONTAINER", os.path.join(os.getcwd(), "data"))

os.makedirs(DATA_FOLDER, exist_ok=True)

SCRAPER_URL = "https://dados.gov.br/dados/conjuntos-dados/indice-desempenho-atendimento"