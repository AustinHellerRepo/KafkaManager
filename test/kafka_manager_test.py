import unittest
from src.austin_heller_repo.kafka_manager import KafkaManager, AsyncHandle, KafkaReader
from austin_heller_repo.threading import start_thread
import uuid
import time
from datetime import datetime


class KafkaManagerTest(unittest.TestCase):

	def setUp(self) -> None:

		print(f"setUp: started: {datetime.utcnow()}")

		kafka_manager = KafkaManager(
			host_url="0.0.0.0",
			host_port=9092,
			read_polling_seconds=1.0,
			cluster_propagation_seconds=30
		)

		print(f"setUp: initialized: {datetime.utcnow()}")

		topics = kafka_manager.get_topics()

		print(f"setUp: get_topics: {datetime.utcnow()}")

		for topic in topics:

			print(f"setUp: topic: {topic}: {datetime.utcnow()}")

			async_handle = kafka_manager.remove_topic(
				topic_name=topic,
				cluster_propagation_blocking_timeout_seconds=30
			)

			print(f"setUp: async: {topic}: {datetime.utcnow()}")

			async_handle.get_result()

			print(f"setUp: result: {topic}: {datetime.utcnow()}")

	def test_initialize(self):

		kafka_manager = KafkaManager(
			host_url="0.0.0.0",
			host_port=9092,
			read_polling_seconds=1.0,
			cluster_propagation_seconds=30
		)

		self.assertIsNotNone(kafka_manager)

	def test_list_all_topics(self):

		kafka_manager = KafkaManager(
			host_url="0.0.0.0",
			host_port=9092,
			read_polling_seconds=1.0,
			cluster_propagation_seconds=30
		)

		topics = kafka_manager.get_topics()

		print(f"topics: {topics}")

		self.assertEqual((), topics)

	def test_add_topic(self):

		print(f"test_add_topic: start: {datetime.utcnow()}")

		kafka_manager = KafkaManager(
			host_url="0.0.0.0",
			host_port=9092,
			read_polling_seconds=1.0,
			cluster_propagation_seconds=0
		)

		print(f"test_add_topic: initialized: {datetime.utcnow()}")

		topic_name = str(uuid.uuid4())

		print(f"topic_name: {topic_name}")

		add_topic_async_handle = kafka_manager.add_topic(
			topic_name=topic_name,
			partition_total=1,
			replication_factor=1
		)

		added_topic_name = add_topic_async_handle.get_result()

		self.assertEqual(topic_name, added_topic_name)

		topics = kafka_manager.get_topics()

		print(f"topics: {topics}")

		self.assertEqual((topic_name,), topics)

	def test_write_and_read_from_topic_transactional(self):

		kafka_manager = KafkaManager(
			host_url="0.0.0.0",
			host_port=9092,
			read_polling_seconds=1.0,
			cluster_propagation_seconds=30
		)

		topic_name = str(uuid.uuid4())

		print(f"topic_name: {topic_name}")

		kafka_manager.add_topic(
			topic_name=topic_name,
			partition_total=1,
			replication_factor=1
		).get_result()

		group_name = str(uuid.uuid4())

		print(f"group name: {group_name}")

		unexpected_message = b"unexpected"
		unexpected_written_message = None

		def write_unexpected_thread_method():
			nonlocal unexpected_message
			nonlocal unexpected_written_message

			kafka_writer = kafka_manager.get_transactional_writer()

			write_message_async_handle = kafka_writer.write_message(
				topic_name=topic_name,
				partition_index=0,
				message_bytes=unexpected_message
			)

			kafka_writer.end_write_transaction()

			unexpected_written_message = write_message_async_handle.get_result()

			print(f"unexpected_written_message: {unexpected_written_message}")

		kafka_reader = None  # type: KafkaReader

		def create_read_message_async_handle_thread_method():
			nonlocal kafka_reader

			kafka_reader = kafka_manager.get_reader(
				topic_name=topic_name,
				group_name=group_name,
				is_from_beginning=False
			)

		expected_message = b"hello world"
		expected_written_message = None
		expected_written_message_async_handles = []  # type: List[AsyncHandle]

		def write_expected_thread_method():
			nonlocal expected_message
			nonlocal expected_written_message
			nonlocal expected_written_message_async_handles

			for index in range(10):
				kafka_writer = kafka_manager.get_transactional_writer()
				write_message_async_handle = kafka_writer.write_message(
					topic_name=topic_name,
					partition_index=0,
					message_bytes=f"message #{index}".encode()
				)
				kafka_writer.end_write_transaction()
				expected_written_message_async_handles.append(write_message_async_handle)

			for expected_written_message_async_handle in expected_written_message_async_handles:
				expected_written_message = expected_written_message_async_handle.get_result()
				print(f"written_message: {expected_written_message}")

		read_message = None

		def read_thread_method():
			nonlocal expected_message
			nonlocal read_message

			print("reading")
			kafka_reader_async_handle = kafka_reader.read_message()
			print(f"getting result")
			read_message = kafka_reader_async_handle.get_result()
			print(f"read message: {read_message}")

		write_unexpected_thread = start_thread(write_unexpected_thread_method)

		write_unexpected_thread.join()

		create_read_message_async_handle_thread = start_thread(create_read_message_async_handle_thread_method)

		create_read_message_async_handle_thread.join()

		read_threads = []
		for index in range(10):
			read_thread = start_thread(read_thread_method)
			read_threads.append(read_thread)

		write_expected_thread = start_thread(write_expected_thread_method)

		write_expected_thread.join()

		for read_thread in read_threads:
			read_thread.join()

		self.assertEqual(unexpected_message, unexpected_written_message)
		self.assertEqual(expected_message, expected_written_message)
		self.assertEqual(expected_message, read_message)

	def test_write_and_read_from_topic_async(self):

		kafka_manager = KafkaManager(
			host_url="0.0.0.0",
			host_port=9092,
			read_polling_seconds=1.0,
			cluster_propagation_seconds=30
		)

		topic_name = str(uuid.uuid4())

		print(f"topic_name: {topic_name}")

		kafka_manager.add_topic(
			topic_name=topic_name,
			partition_total=1,
			replication_factor=1
		).get_result()

		group_name = str(uuid.uuid4())

		print(f"group name: {group_name}")

		unexpected_message = b"unexpected"
		unexpected_written_message = None

		def write_unexpected_thread_method():
			nonlocal unexpected_message
			nonlocal unexpected_written_message

			kafka_writer = kafka_manager.get_async_writer()

			write_message_async_handle = kafka_writer.write_message(
				topic_name=topic_name,
				partition_index=0,
				message_bytes=unexpected_message
			)

			unexpected_written_message = write_message_async_handle.get_result()

			print(f"unexpected_written_message: {unexpected_written_message}")

		kafka_reader = None  # type: KafkaReader

		def create_read_message_async_handle_thread_method():
			nonlocal kafka_reader

			kafka_reader = kafka_manager.get_reader(
				topic_name=topic_name,
				group_name=group_name,
				is_from_beginning=False
			)

		expected_message = b"hello world"
		expected_written_message = None
		expected_written_message_async_handles = []  # type: List[AsyncHandle]

		def write_expected_thread_method():
			nonlocal expected_message
			nonlocal expected_written_message
			nonlocal expected_written_message_async_handles

			for index in range(10):
				kafka_writer = kafka_manager.get_transactional_writer()
				write_message_async_handle = kafka_writer.write_message(
					topic_name=topic_name,
					partition_index=0,
					message_bytes=f"message #{index}".encode()
				)
				kafka_writer.end_write_transaction()
				expected_written_message_async_handles.append(write_message_async_handle)

			for expected_written_message_async_handle in expected_written_message_async_handles:
				expected_written_message = expected_written_message_async_handle.get_result()
				print(f"written_message: {expected_written_message}")

		read_message = None

		def read_thread_method():
			nonlocal expected_message
			nonlocal read_message

			print("reading")
			kafka_reader_async_handle = kafka_reader.read_message()
			print(f"getting result")
			read_message = kafka_reader_async_handle.get_result()
			print(f"read message: {read_message}")

		write_unexpected_thread = start_thread(write_unexpected_thread_method)

		write_unexpected_thread.join()

		create_read_message_async_handle_thread = start_thread(create_read_message_async_handle_thread_method)

		create_read_message_async_handle_thread.join()

		read_threads = []
		for index in range(10):
			read_thread = start_thread(read_thread_method)
			read_threads.append(read_thread)

		write_expected_thread = start_thread(write_expected_thread_method)

		write_expected_thread.join()

		for read_thread in read_threads:
			read_thread.join()

		self.assertEqual(unexpected_message, unexpected_written_message)
		self.assertEqual(expected_message, expected_written_message)
		self.assertEqual(expected_message, read_message)
