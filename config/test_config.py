
class TestConfig:
    ENDPOINT_URL = "http://localhost:9000/test"

    @staticmethod
    def config():
        config = {"ENDPOINT_URL": TestConfig.ENDPOINT_URL}
        return config
