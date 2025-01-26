#!/usr/bin/env python
import os
import pathlib
from configparser import ConfigParser
import ccxt

#global exchange

def get_api_key():
    config = ConfigParser()
    config_file_path = os.path.join(
        pathlib.Path(__file__).parent.resolve(), "..", "config.ini"
    )
    config.read(config_file_path)
    return config["keys"]["api_key"], config["keys"]["api_secret"]

def get_test_url():
    config = ConfigParser()
    config_file_path = os.path.join(
        pathlib.Path(__file__).parent.resolve(), "..", "config.ini"
    )
    config.read(config_file_path)
    return config["url"]["test_api"]

def get_telegram_token():
    config = ConfigParser()
    config_file_path = os.path.join(
        pathlib.Path(__file__).parent.resolve(), "..", "config.ini"
    )
    config.read(config_file_path)
    return config["telegram"]["token"]

def get_telegram_chatId():
    config = ConfigParser()
    config_file_path = os.path.join(
        pathlib.Path(__file__).parent.resolve(), "..", "config.ini"
    )
    config.read(config_file_path)
    return config["telegram"]["chatId"]

def get_ccy_target():
    config = ConfigParser()
    config_file_path = os.path.join(
        pathlib.Path(__file__).parent.resolve(), "..", "config.ini"
    )
    config.read(config_file_path)
    return config["asset"]["target"]

def get_ccy_home():
    config = ConfigParser()
    config_file_path = os.path.join(
        pathlib.Path(__file__).parent.resolve(), "..", "config.ini"
    )
    config.read(config_file_path)
    return config["asset"]["home"]

def get_ccy_pair():
    config = ConfigParser()
    config_file_path = os.path.join(
        pathlib.Path(__file__).parent.resolve(), "..", "config.ini"
    )
    config.read(config_file_path)
    return config["asset"]["target"]+config["asset"]["home"]

def get_ccxt_exchange():
    exchange = ccxt.binance()
    exchange.set_sandbox_mode(True)
    api_key, api_secret = get_api_key()
    exchange.apiKey = api_key
    exchange.secret = api_secret
    return exchange
    
