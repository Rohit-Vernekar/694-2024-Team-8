import yaml

with open("config.yml") as fp:
    config = yaml.safe_load(fp)
    cache_config = config["cache"]
