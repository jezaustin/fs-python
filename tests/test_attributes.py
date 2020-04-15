import unittest


class ClassWithAttribute:

    def __init__(self, topic_name="test"):
        self.topic = topic_name

    def __getattribute__(self, name):
        return object.__getattribute__(self, name)


class ClassWithMethod:

    # Note - attribute must have _ prefix so no "clash" with method name!

    def __init__(self, topic_name="test"):
        self._topic = topic_name

    def topic(self):
        return self._topic


class TestAttributes(unittest.TestCase):

    def setUp(self):
        self.class_with_attribute = ClassWithAttribute("test1")
        self.class_with_method = ClassWithMethod("test2")

    def tearDown(self):
        pass

    def test_class_with_attribute(self):
        self.assertEqual("test1", self.class_with_attribute.topic)

    def test_class_with_method(self):
        print(self.class_with_method.topic())
        self.assertEqual("test2", self.class_with_method.topic())
