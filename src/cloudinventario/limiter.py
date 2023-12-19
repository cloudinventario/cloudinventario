class Singleton(object):
  _instances = {}
  def __new__(class_, *args, **kwargs):
    if class_ not in class_._instances:
        class_._instances[class_] = super(Singleton, class_).__new__(class_, *args, **kwargs)
    return class_._instances[class_]

class CloudInventarioLimiter(Singleton):
    def __init__(self):
        self.sources = {}

    def add_source(self, name, config, counter=0):
        if name not in self.sources:
            self.sources[name] = {
                "inventory-limit": config['inventory-limit'],
                "counter": counter
            }

    def add_counter(self, name, config):
        if name in self.sources:
            counter = self.sources[name]['counter']
            limit = self.sources[name]['inventory-limit']

            if (counter + 1) > limit:
                return False, f'Source {name} reached limit for collecting'
            self.sources[name]['counter'] = counter + 1
        else:
            self.add_source(name, config, counter=1)
        return True, ''