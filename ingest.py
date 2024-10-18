import logging
import json
import requests
import time
from omegaconf import OmegaConf, DictConfig
import toml
import sys
import os
from typing import Any
import importlib

from core.crawler import Crawler
from core.utils import setup_logging
from authlib.integrations.requests_client import OAuth2Session

def instantiate_crawler(base_class, folder_name: str, class_name: str, *args, **kwargs) -> Any:
    logging.debug('Inside instantiate_crawler')
    sys.path.insert(0, os.path.abspath(folder_name))

    crawler_name = class_name.split('Crawler')[0]
    module_name = f"{folder_name}.{crawler_name.lower()}_crawler"

    try:
        module = importlib.import_module(module_name)
        class_ = getattr(module, class_name)
        logging.debug(f"Successfully imported {class_name} from {module_name}")
    except (ImportError, AttributeError) as e:
        logging.error(f"Error importing {class_name} from {module_name}: {e}")
        raise

    if not issubclass(class_, base_class):
        error_msg = f"{class_name} is not a subclass of {base_class.__name__}"
        logging.error(error_msg)
        raise TypeError(error_msg)

    logging.debug('Crawler class instantiated successfully')
    return class_(*args, **kwargs)

def get_jwt_token(auth_url: str, auth_id: str, auth_secret: str, customer_id: str) -> Any:
    logging.debug('Fetching JWT token...')
    token_endpoint = f'{auth_url}/oauth2/token'
    session = OAuth2Session(auth_id, auth_secret, scope="")

    try:
        token = session.fetch_token(token_endpoint, grant_type="client_credentials")
        logging.debug('JWT token fetched successfully')
        return token["access_token"]
    except Exception as e:
        logging.error(f"Failed to fetch JWT token: {e}")
        raise

def reset_corpus(endpoint: str, customer_id: str, corpus_id: int, auth_url: str, auth_id: str, auth_secret: str) -> None:
    """Reset the corpus by deleting all documents and metadata."""
    url = f"https://{endpoint}/v1/reset-corpus"
    payload = json.dumps({"customerId": customer_id, "corpusId": corpus_id})
    token = get_jwt_token(auth_url, auth_id, auth_secret, customer_id)

    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'customer-id': str(customer_id),
        'Authorization': f'Bearer {token}'
    }

    response = requests.post(url, headers=headers, data=payload)
    if response.status_code == 200:
        logging.info(f"Reset corpus {corpus_id}")
    else:
        logging.error(f"Error resetting corpus: {response.status_code} {response.text}")

def main() -> None:
    if len(sys.argv) != 3:
        logging.info("Usage: python ingest.py <config_file> <secrets-profile>")
        return

    logging.info("Starting the Crawler...")
    config_name = sys.argv[1]
    profile_name = sys.argv[2]

    cfg: DictConfig = DictConfig(OmegaConf.load(config_name))

    # Load secrets from .toml file
    volume = '/home/vectara/env'
    with open(f"{volume}/secrets.toml", 'r') as f:
        env_dict = toml.load(f)

    if profile_name not in env_dict:
        logging.info(f'Profile "{profile_name}" not found in secrets.toml')
        return

    logging.info(f'Using profile "{profile_name}" from secrets.toml')
    general_dict = env_dict.get('general', {})
    for k, v in general_dict.items():
        OmegaConf.update(cfg, f'vectara.{k.lower()}', v)

    env_dict = env_dict[profile_name]
    for k, v in env_dict.items():
        if k == 'HUBSPOT_API_KEY':
            OmegaConf.update(cfg, f'hubspot_crawler.{k.lower()}', v)
        elif k == 'NOTION_API_KEY':
            OmegaConf.update(cfg, f'notion_crawler.{k.lower()}', v)
        elif k == 'SLACK_USER_TOKEN':
            OmegaConf.update(cfg, f'slack_crawler.{k.lower()}', v)
        elif k == 'DISCOURSE_API_KEY':
            OmegaConf.update(cfg, f'discourse_crawler.{k.lower()}', v)
        elif k == 'FMP_API_KEY':
            OmegaConf.update(cfg, f'fmp_crawler.{k.lower()}', v)
        elif k == 'JIRA_PASSWORD':
            OmegaConf.update(cfg, f'jira_crawler.{k.lower()}', v)
        elif k == 'GITHUB_TOKEN':
            OmegaConf.update(cfg, f'github_crawler.{k.lower()}', v)
        elif k == 'SYNAPSE_TOKEN':
            OmegaConf.update(cfg, f'synapse_crawler.{k.lower()}', v)
        elif k == 'TWITTER_BEARER_TOKEN':
            OmegaConf.update(cfg, f'twitter_crawler.{k.lower()}', v)
        elif k.startswith('aws_'):
            OmegaConf.update(cfg, f's3_crawler.{k.lower()}', v)
        else:
            OmegaConf.update(cfg['vectara'], k, v)

    logging.info("Configuration loaded...")
    endpoint = cfg.vectara.get("endpoint", "api.vectara.io")
    customer_id = cfg.vectara.customer_id
    corpus_id = cfg.vectara.corpus_id
    api_key = cfg.vectara.api_key
    crawler_type = cfg.crawling.crawler_type

    crawler = instantiate_crawler(
        Crawler, 'crawlers', f'{crawler_type.capitalize()}Crawler',
        cfg, endpoint, customer_id, corpus_id, api_key
    )

    logging.info("Crawling instantiated...")
    reset_corpus_flag = False
    if reset_corpus_flag:
        logging.info("Resetting corpus")
        reset_corpus(endpoint, customer_id, corpus_id, cfg.vectara.auth_url, cfg.vectara.auth_id, cfg.vectara.auth_secret)
        time.sleep(5)

    logging.info(f"Starting crawl of type {crawler_type}...")
    crawler.crawl()
    logging.info(f"Finished crawl of type {crawler_type}...")

if __name__ == '__main__':
    setup_logging()
    main()
