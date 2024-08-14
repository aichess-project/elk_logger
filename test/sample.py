from elk_logger.elk_logger import ELK_Logger

# Assuming your YAML configuration file is located at 'config.yaml'
elk_logger = ELK_Logger('../config/default.yaml')

elk_logger.info('This is an informational message')
elk_logger.info('This is status', status = 100)
elk_logger.info('All', status = 100, function = "Func", variable = "x2", value = 1e-6)
elk_logger.error('This is an error message')
