
class LocalConfig:
    ENDPOINT_URL = "http://localhost:9000/consumer_reporting_endpoint"

    @staticmethod
    def config():
        config = {"ENDPOINT_URL": LocalConfig.ENDPOINT_URL}
        return config
