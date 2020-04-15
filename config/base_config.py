
class BaseConfig:
    ENDPOINT_URL = "http://focussensors.duckdns.org:9000/consumer_reporting_endpoint"

    @staticmethod
    def config():
        config = {"ENDPOINT_URL": BaseConfig.ENDPOINT_URL}
        return config
