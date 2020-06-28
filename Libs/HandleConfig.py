import json


class HandleConfig:
    config_path = 'Config'

    @staticmethod
    def read(name):
        config_file = open('Config/%s.json' % (name))
        return json.load(config_file)


if __name__ == "__main__":
    print(HandleConfig.read('bank'))
