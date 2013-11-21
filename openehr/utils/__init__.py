import logging


def get_logger(logger_label,
               log_format='%(asctime)s|%(levelname)-8s|%(message)s',
               log_datefmt='%Y-%m-%d %H:%M:%S',
               log_level='INFO',
               log_file=None):
    log_level = getattr(logging, log_level)
    kwargs = {
        'format': log_format,
        'datefmt': log_datefmt,
        'level': log_level
    }
    if log_file:
        kwargs['filename'] = log_file
    logging.basicConfig(**kwargs)
    logger = logging.getLogger(logger_label)
    return logger


# transform unicodes in list to strings
def decode_list(data):
    decoded = []
    for item in data:
        if isinstance(item, unicode):
            item = item.encode('utf-8')
        elif isinstance(item, list):
            item = decode_list(item)
        elif isinstance(item, dict):
            item = decode_dict(item)
        decoded.append(item)
    return decoded


# transform unicodes in dictionary to strings (both in keys and values)
def decode_dict(data):
    decoded = {}
    for key, val in data.iteritems():
        if isinstance(key, unicode):
            key = key.encode('utf-8')
        if isinstance(val, unicode):
            val = val.encode('utf-8')
        elif isinstance(val, list):
            val = decode_list(val)
        elif isinstance(val, dict):
            val = decode_dict(val)
        decoded[key] = val
    return decoded