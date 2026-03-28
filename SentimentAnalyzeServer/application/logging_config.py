import logging
import os

def get_app_logger(log_name: str = "app", log_dir: str = None) -> logging.Logger:
    if log_dir is None:
        base_dir = os.path.dirname(os.path.dirname(__file__))
        log_dir = os.path.join(base_dir, 'system', 'logs')
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f'{log_name}.log')
    # 清空原有日志
    with open(log_file, 'w', encoding='utf-8'):
        pass
    logger = logging.getLogger(log_name)
    logger.handlers.clear()
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    formatter = logging.Formatter('[%(asctime)s] %(levelname)s %(name)s: %(message)s')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.setLevel(logging.INFO)
    return logger
