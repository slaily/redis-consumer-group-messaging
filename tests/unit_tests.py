import unittest

from src import utils


class TestUtils(unittest.TestCase):
    def test_cli_parse_args_custom(self):
        # Test with custom arguments
        test_args = [
            "--consumer-group-size",
            "5",
            "--redis-host",
            "redis.example.com",
            "--redis-port",
            "6380",
        ]
        args = utils.cli_parse_args(args=test_args)
        self.assertEqual(args.consumer_group_size, 5)
        self.assertEqual(args.redis_host, "redis.example.com")
        self.assertEqual(args.redis_port, 6380)

    def test_message_handler(self):
        # Test message_handler function
        input_message = {"key": "value"}
        processed_message = utils.message_handler(input_message)

        self.assertIn("key", processed_message)
        self.assertEqual(processed_message["key"], "value")
        self.assertIn("metadata", processed_message)
        self.assertIn("random_property", processed_message["metadata"])
        self.assertIsInstance(processed_message["metadata"]["random_property"], float)
        self.assertTrue(0 <= processed_message["metadata"]["random_property"] < 1)
