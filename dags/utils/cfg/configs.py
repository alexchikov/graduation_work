from yaml import safe_load
import os


class Config(object):
    with open(os.environ['CONFIG_PATH']) as config_file:
        config = safe_load(config_file)

    @classmethod
    def get(cls, key):
        return cls.config[key]
