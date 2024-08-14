import logging
import sys
import requests
import yaml
import datetime
from logging.handlers import RotatingFileHandler

ERROR_LOGSTASH_UNAVAILABLE = "ELK is unavailable"
STATUS_OK = 0

class ELK_Logger(logging.Handler):
    def __init__(self, yaml_config_file, level=logging.INFO):
        super().__init__(level)
        self.level = level
        self.fallback_logger = None
        self.config = self.load_config(yaml_config_file)
        self._initialize_fallback_logger()

    def load_config(self, yaml_config_file):
        try:
            with open(yaml_config_file, 'r') as file:
                config = yaml.safe_load(file)
            return config
        except FileNotFoundError:
            print(f"Error: Configuration file not found at {yaml_config_file}.")
        except yaml.YAMLError as e:
            print(f"Error: Failed to parse YAML file: {e}")
        except Exception as e:
            print(f"Error: An unexpected error occurred while loading the configuration: {e}")
        sys.exit(1)

    def emit(self, record):
        log_message = {
            'timestamp': datetime.datetime.now().isoformat(),
            'message': record.getMessage(),
        }
        success = self.send_to_logstash(log_message)
        if not success:
            self._fallback(log_message, ERROR_LOGSTASH_UNAVAILABLE)

    def send_to_logstash(self, log_message):
        try:
            response = requests.post(self.config['logstash_url'], json=log_message)
            if response.status_code == 200:
                return True
            print(f"Failed to send log to Logstash: {response.text}")
        except Exception as e:
            print(f"Error sending log to Logstash: {e}")
        return False

    def _fallback(self, record, reason):
        fallback_message = f"{record} (FALLBACK due to: {reason})"
        print(fallback_message)
        self.fallback_logger.log(self.level, fallback_message)

    def _initialize_fallback_logger(self):
        self.fallback_logger = logging.getLogger('fallback')
        handler = RotatingFileHandler(
            self.config['fallback_file'], maxBytes=self.config['max_file_size'], backupCount=self.config['backup_count']
        )
        handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        self.fallback_logger.addHandler(handler)
        self.fallback_logger.setLevel(self.level)

    def compose_log_message(self, message = '', status = '', variable = '', value = ''):
        log_message = {
            'message': message,
            'status': status,
            'variable': variable,
            'value': value
        }
        return log_message
