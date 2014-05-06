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


def decode_list(data):
    """
    Transforms UNICODE in list into strings

    :param: data the list with unicode values that must be converted
    :type data: list
    :return: the list with converted unicode
    :rtype: list
    """
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


def decode_dict(data):
    """
    Transforms UNICODE in dictionary (both in keys and values) into strings

    :param data: the dictionary with unicode that must be converted
    :type data: dictionary
    :return: the dictionary with unicode replaced by strings
    :rtype: dictionary
    """
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


def cleanup_json(data):
    """
    Remove all keys with a None value from the given JSON dictionary
    in order to avoid validation errors

    :param data: the dictionary that is going to be cleaned up
    :type data: dictionary
    :return: the cleaned dictionary
    :rtype: dictionary
    """
    cleanead_data = {}
    for k, v in data.iteritems():
        if not v is None:
            cleanead_data[k] = v
    return cleanead_data