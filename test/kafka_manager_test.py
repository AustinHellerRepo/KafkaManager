import unittest
from src.austin_heller_repo.kafka_manager import KafkaManager, AsyncHandle, KafkaReader, KafkaWrapper
from austin_heller_repo.threading import start_thread, Semaphore
import uuid
import time
from datetime import datetime
from typing import List, Tuple, Dict


class KafkaManagerTest(unittest.TestCase):

	def setUp(self) -> None:

		print(f"setUp: started: {datetime.utcnow()}")

		kafka_wrapper = KafkaWrapper(
			host_url="0.0.0.0",
			host_port=9092
		)

		kafka_manager = KafkaManager(
			kafka_wrapper=kafka_wrapper,
			read_polling_seconds=1.0,
			cluster_propagation_seconds=30,
			new_topic_partitions_total=1,
			new_topic_replication_factor=1,
			remove_topic_cluster_propagation_blocking_timeout_seconds=30
		)

		print(f"setUp: initialized: {datetime.utcnow()}")

		topics = kafka_manager.get_topics()

		print(f"setUp: get_topics: {datetime.utcnow()}")

		for topic in topics:

			print(f"setUp: topic: {topic}: {datetime.utcnow()}")

			async_handle = kafka_manager.remove_topic(
				topic_name=topic
			)

			print(f"setUp: async: {topic}: {datetime.utcnow()}")

			async_handle.get_result()

			print(f"setUp: result: {topic}: {datetime.utcnow()}")

	def test_initialize(self):

		kafka_wrapper = KafkaWrapper(
			host_url="0.0.0.0",
			host_port=9092
		)

		kafka_manager = KafkaManager(
			kafka_wrapper=kafka_wrapper,
			read_polling_seconds=1.0,
			cluster_propagation_seconds=30,
			new_topic_partitions_total=1,
			new_topic_replication_factor=1,
			remove_topic_cluster_propagation_blocking_timeout_seconds=30
		)

		self.assertIsNotNone(kafka_manager)

	def test_list_all_topics(self):

		kafka_wrapper = KafkaWrapper(
			host_url="0.0.0.0",
			host_port=9092
		)

		kafka_manager = KafkaManager(
			kafka_wrapper=kafka_wrapper,
			read_polling_seconds=1.0,
			cluster_propagation_seconds=30,
			new_topic_partitions_total=1,
			new_topic_replication_factor=1,
			remove_topic_cluster_propagation_blocking_timeout_seconds=30
		)

		topics = kafka_manager.get_topics()

		print(f"topics: {topics}")

		self.assertEqual((), topics)

	def test_add_and_remove_topic(self):

		kafka_wrapper = KafkaWrapper(
			host_url="0.0.0.0",
			host_port=9092
		)

		kafka_manager = KafkaManager(
			kafka_wrapper=kafka_wrapper,
			read_polling_seconds=1.0,
			cluster_propagation_seconds=30,
			new_topic_partitions_total=1,
			new_topic_replication_factor=1,
			remove_topic_cluster_propagation_blocking_timeout_seconds=30
		)

		topic_name = str(uuid.uuid4())

		add_topic_async_handle = kafka_manager.add_topic(
			topic_name=topic_name
		)

		added_topic_name = add_topic_async_handle.get_result()

		self.assertEqual(topic_name, added_topic_name)

		topics = kafka_manager.get_topics()

		self.assertEqual((topic_name,), topics)

		removed_topic_name = kafka_manager.remove_topic(
			topic_name=topic_name
		).get_result()

		self.assertEqual(topic_name, removed_topic_name)

		topics = kafka_manager.get_topics()

		self.assertEqual((), topics)

	def test_write_and_read_from_topic_transactional(self):

		kafka_wrapper = KafkaWrapper(
			host_url="0.0.0.0",
			host_port=9092
		)

		kafka_manager = KafkaManager(
			kafka_wrapper=kafka_wrapper,
			read_polling_seconds=1.0,
			cluster_propagation_seconds=30,
			new_topic_partitions_total=1,
			new_topic_replication_factor=1,
			remove_topic_cluster_propagation_blocking_timeout_seconds=30
		)

		topic_name = str(uuid.uuid4())

		print(f"topic_name: {topic_name}")

		kafka_manager.add_topic(
			topic_name=topic_name
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

		expected_written_message = None
		expected_written_message_async_handles = []  # type: List[AsyncHandle]
		expected_messages = []  # type: List[bytes]
		expected_messages_semaphore = Semaphore()

		def write_expected_thread_method():
			nonlocal expected_written_message
			nonlocal expected_written_message_async_handles
			nonlocal expected_messages
			nonlocal expected_messages_semaphore

			for index in range(10):
				kafka_writer = kafka_manager.get_transactional_writer()
				write_message_async_handle = kafka_writer.write_message(
					topic_name=topic_name,
					message_bytes=f"message #{index}".encode()
				)
				kafka_writer.end_write_transaction()
				expected_written_message_async_handles.append(write_message_async_handle)

			for expected_written_message_async_handle in expected_written_message_async_handles:
				expected_written_message = expected_written_message_async_handle.get_result()
				print(f"written_message: {expected_written_message}")
				expected_messages_semaphore.acquire()
				expected_messages.append(expected_written_message)
				expected_messages_semaphore.release()

		read_message = None
		read_messages = []  # type: List[bytes]
		read_messages_semaphore = Semaphore()

		def read_thread_method():
			nonlocal read_message
			nonlocal read_messages
			nonlocal read_messages_semaphore

			print("reading")
			kafka_reader_async_handle = kafka_reader.read_message()
			print(f"getting result")
			read_message = kafka_reader_async_handle.get_result()
			print(f"read message: {read_message}")
			read_messages_semaphore.acquire()
			read_messages.append(read_message)
			read_messages_semaphore.release()

		write_unexpected_thread = start_thread(write_unexpected_thread_method)

		write_unexpected_thread.join()

		create_read_message_async_handle_thread = start_thread(create_read_message_async_handle_thread_method)

		create_read_message_async_handle_thread.join()

		read_threads = []
		for index in range(10):
			read_thread = start_thread(read_thread_method)
			read_threads.append(read_thread)

		time.sleep(10)

		write_expected_thread = start_thread(write_expected_thread_method)

		write_expected_thread.join()

		for read_thread in read_threads:
			read_thread.join()

		self.assertEqual(unexpected_message, unexpected_written_message)
		for expected_message in expected_messages:
			self.assertIn(expected_message, read_messages)
		for read_message in read_messages:
			self.assertIn(read_message, expected_messages)

	def test_write_and_read_from_topic_async(self):

		kafka_wrapper = KafkaWrapper(
			host_url="0.0.0.0",
			host_port=9092
		)

		kafka_manager = KafkaManager(
			kafka_wrapper=kafka_wrapper,
			read_polling_seconds=1.0,
			cluster_propagation_seconds=30,
			new_topic_partitions_total=1,
			new_topic_replication_factor=1,
			remove_topic_cluster_propagation_blocking_timeout_seconds=30
		)

		topic_name = str(uuid.uuid4())

		print(f"topic_name: {topic_name}")

		kafka_manager.add_topic(
			topic_name=topic_name
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

		expected_written_message = None
		expected_written_message_async_handles = []  # type: List[AsyncHandle]
		expected_messages = []  # type: List[bytes]
		expected_messages_semaphore = Semaphore()

		def write_expected_thread_method():
			nonlocal expected_written_message
			nonlocal expected_written_message_async_handles
			nonlocal expected_messages
			nonlocal expected_messages_semaphore

			for index in range(10):
				kafka_writer = kafka_manager.get_async_writer()
				write_message_async_handle = kafka_writer.write_message(
					topic_name=topic_name,
					message_bytes=f"message #{index}".encode()
				)
				expected_written_message_async_handles.append(write_message_async_handle)

			for expected_written_message_async_handle in expected_written_message_async_handles:
				expected_written_message = expected_written_message_async_handle.get_result()
				print(f"written_message: {expected_written_message}")
				expected_messages_semaphore.acquire()
				expected_messages.append(expected_written_message)
				expected_messages_semaphore.release()

		read_message = None
		read_messages = []  # type: List[bytes]
		read_messages_semaphore = Semaphore()

		def read_thread_method():
			nonlocal read_message
			nonlocal read_messages
			nonlocal read_messages_semaphore

			print("reading")
			kafka_reader_async_handle = kafka_reader.read_message()
			print(f"getting result")
			read_message = kafka_reader_async_handle.get_result()
			print(f"read message: {read_message}")
			read_messages_semaphore.acquire()
			read_messages.append(read_message)
			read_messages_semaphore.release()

		write_unexpected_thread = start_thread(write_unexpected_thread_method)

		write_unexpected_thread.join()

		create_read_message_async_handle_thread = start_thread(create_read_message_async_handle_thread_method)

		create_read_message_async_handle_thread.join()

		read_threads = []
		for index in range(10):
			read_thread = start_thread(read_thread_method)
			read_threads.append(read_thread)

		# wait for the readers to have a change to poll, causing them to be assigned
		time.sleep(10)

		write_expected_thread = start_thread(write_expected_thread_method)

		write_expected_thread.join()

		for read_thread in read_threads:
			read_thread.join()

		self.assertEqual(unexpected_message, unexpected_written_message)
		for expected_message in expected_messages:
			self.assertIn(expected_message, read_messages)
		for read_message in read_messages:
			self.assertIn(read_message, expected_messages)
