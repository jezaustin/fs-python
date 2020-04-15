
class TestConfig:
    ENDPOINT_URL = "print"

    @staticmethod
    def config():
        config = {"ENDPOINT_URL": TestConfig.ENDPOINT_URL}
        return config
