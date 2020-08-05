import yaml


class HandleConfig:
    config_path = 'Config'

    @staticmethod
    def read(name, file_type='json'):
        config_file = open('Config/%s.%s' % (name, file_type))
        return yaml.load(config_file)
    


if __name__ == "__main__":
    print(HandleConfig.read('bank'))
