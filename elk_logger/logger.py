import logging
import sys
import requests
import yaml
import datetime
import json
from pythonjsonlogger import jsonlogger
from logging.handlers import RotatingFileHandler

class LogJsonFormatter(jsonlogger.JsonFormatter):
    def __init__(self, *args, **kwargs):
        # Define the log message format you want
        log_format = '%(asctime)s %(levelname)s %(name)s %(message)s'
        super(LogJsonFormatter, self).__init__(fmt=log_format, *args, **kwargs)

    def add_fields(self, log_record, record, message_dict):
        super(LogJsonFormatter, self).add_fields(log_record, record, message_dict)
        log_record['timestamp'] = datetime.datetime.utcnow().isoformat() + "Z"
        log_record['level'] = record.levelname
        log_record['logger_name'] = record.name
        log_record['status'] = record.__dict__.get('status', None)
        log_record['function'] = record.__dict__.get('function', None)
        log_record['variable'] = record.__dict__.get('variable', None)
        log_record['value'] = record.__dict__.get('value', None)

class ELKHandler(logging.Handler):
    def __init__(self, elk_url, elk_index):
        super().__init__()
        self.elk_url = elk_url
        self.elk_index = elk_index
        self.formatter = LogJsonFormatter()

    def emit(self, record):
        log_message = self.format(record)
        headers = {
            'Content-Type': 'application/json'
        }
        try:
            response = requests.post(f"{self.elk_url}/{self.elk_index}", headers=headers, data=log_message)
            if response.status_code != 200:
                self.handleError(record)
        except Exception as e:
            self.handleError(record)

class ELK_Logger:
    def __init__(self, config_file: str):
        self.formatter = LogJsonFormatter()
        self.config = self._load_config(config_file)
        self.logger = logging.getLogger("ELK_Logger")
        self._configure_logger()

    def _load_config(self, config_file: str):
        try:
            with open(config_file, 'r') as file:
                config = yaml.safe_load(file)
            return config
        except FileNotFoundError:
            print(f"Error: Configuration file not found: {config_file}")
            sys.exit(1)
        except yaml.YAMLError as e:
            print(f"Error: Failed to parse YAML file: {e}")
            sys.exit(1)
        except Exception as e:
            print(f"Error: An unexpected error occurred while loading the configuration: {e}")
            sys.exit(1)

    def _configure_logger(self):
        # Set logging level
        log_level = getattr(logging, self.config.get('log_level', 'INFO').upper(), logging.INFO)
        self.logger.setLevel(log_level)

        # Configure file logging
        file_handler = RotatingFileHandler(
            self.config['file_logging'].get('filename', 'app.log'),
            maxBytes=self.config['file_logging'].get('max_file_size', 5 * 1024 * 1024),
            backupCount=self.config['file_logging'].get('backup_count', 5)
        )
        file_handler.setFormatter(self.formatter)
        self.logger.addHandler(file_handler)

        # Configure ELK/Logstash logging if configured
        if 'elk_logging' in self.config:
            elk_url = self.config['elk_logging'].get('logstash_url')
            elk_index = self.config['elk_logging'].get('index', 'logs')
            if elk_url:
                elk_handler = ELKHandler(elk_url, elk_index)
                elk_handler.setLevel(log_level)
                elk_handler.setFormatter(self.formatter)
                self.logger.addHandler(elk_handler)

    def log(self, level, message: str, status: str = None, function = None, variable: str = None, value = None):
        extra = {
            'status': status,
            'function': function,
            'variable': variable,
            'value': value
        }
        self.logger.log(level, message, extra=extra)

    def debug(self, message: str, status = None, function = None, variable = None, value = None):
        self.log(logging.DEBUG, message, status, function, variable, value)

    def info(self, message: str, status = None, function = None, variable = None, value = None):
        self.log(logging.INFO, message, status, function, variable, value)

    def warning(self, message: str, status = None, function = None, variable = None, value = None):
        self.log(logging.WARNING, message, status, function, variable, value)

    def error(self, message: str, status = None, function = None, variable = None, value = None):
        self.log(logging.ERROR, message, status, function, variable, value)

    def critical(self, message: str, status = None, function = None, variable = None, value = None):
        self.log(logging.CRITICAL, message, status, function, variable, value)
