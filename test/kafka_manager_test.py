import unittest
from src.austin_heller_repo.kafka_manager import KafkaManager, KafkaReader, KafkaWrapper, KafkaTopicSeekIndex, KafkaMessage, KafkaAsyncWriter, KafkaTransactionalWriter
from austin_heller_repo.threading import start_thread, Semaphore, BooleanReference, AsyncHandle
from austin_heller_repo.common import HostPointer
import uuid
import time
from datetime import datetime
from typing import List, Tuple, Dict
import matplotlib.pyplot as plt
import matplotlib


def get_default_kafka_manager() -> KafkaManager:

	kafka_wrapper = KafkaWrapper(
		host_pointer=HostPointer(
			host_address="0.0.0.0",
			host_port=9092
		)
	)

	kafka_manager = KafkaManager(
		kafka_wrapper=kafka_wrapper,
		read_polling_seconds=0.001,
		is_cancelled_polling_seconds=0.001,
		new_topic_partitions_total=1,
		new_topic_replication_factor=1,
		remove_topic_cluster_propagation_blocking_timeout_seconds=30
	)

	return kafka_manager


class KafkaManagerTest(unittest.TestCase):

	def setUp(self) -> None:

		print(f"setUp: started: {datetime.utcnow()}")

		kafka_manager = get_default_kafka_manager()

		print(f"setUp: initialized: {datetime.utcnow()}")

		topics = kafka_manager.get_topics().get_result()  # type: List[str]

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

		kafka_manager = get_default_kafka_manager()

		self.assertIsNotNone(kafka_manager)

		time.sleep(1)

	def test_list_all_topics(self):

		kafka_manager = get_default_kafka_manager()

		topics = kafka_manager.get_topics().get_result()

		print(f"topics: {topics}")

		self.assertEqual((), topics)

		time.sleep(1)

	def test_add_and_remove_topic(self):

		kafka_manager = get_default_kafka_manager()

		topic_name = str(uuid.uuid4())

		add_topic_async_handle = kafka_manager.add_topic(
			topic_name=topic_name
		)

		added_topic_name = add_topic_async_handle.get_result()

		self.assertEqual(topic_name, added_topic_name)

		topics = kafka_manager.get_topics().get_result()

		self.assertEqual((topic_name,), topics)

		removed_topic_name = kafka_manager.remove_topic(
			topic_name=topic_name
		).get_result()

		self.assertEqual(topic_name, removed_topic_name)

		topics = kafka_manager.get_topics().get_result()

		self.assertEqual((), topics)

		time.sleep(1)

	def test_write_and_read_transactional_once(self):

		kafka_manager = get_default_kafka_manager()

		topic_name = str(uuid.uuid4())

		print(f"topic_name: {topic_name}")

		kafka_manager.add_topic(
			topic_name=topic_name
		).get_result()

		expected_message_bytes = b"expected"

		kafka_writer = kafka_manager.get_transactional_writer().get_result()  # type: KafkaTransactionalWriter

		write_message = kafka_writer.write_message(
			topic_name=topic_name,
			message_bytes=expected_message_bytes
		).get_result()  # type: KafkaMessage

		kafka_writer.end_write_transaction()

		kafka_reader = kafka_manager.get_reader(
			topic_name=topic_name,
			is_from_beginning=True
		).get_result()  # type: KafkaReader

		read_message = kafka_reader.read_message().get_result()  # type: KafkaMessage

		self.assertEqual(expected_message_bytes, write_message.get_message_bytes())
		self.assertEqual(expected_message_bytes, read_message.get_message_bytes())

		time.sleep(1)

	def TODO__test_write_and_read_from_topic_transactional(self):

		kafka_manager = get_default_kafka_manager()

		topic_name = str(uuid.uuid4())

		print(f"topic_name: {topic_name}")

		kafka_manager.add_topic(
			topic_name=topic_name
		).get_result()

		unexpected_message = b"unexpected"
		unexpected_written_message = None

		def write_unexpected_thread_method():
			nonlocal unexpected_message
			nonlocal unexpected_written_message

			kafka_writer = kafka_manager.get_transactional_writer().get_result()  # type: KafkaTransactionalWriter

			write_message_async_handle = kafka_writer.write_message(
				topic_name=topic_name,
				message_bytes=unexpected_message
			)

			kafka_writer.end_write_transaction()

			unexpected_written_message = write_message_async_handle.get_result().get_message_bytes()

			print(f"unexpected_written_message: {unexpected_written_message}")

		kafka_reader = None  # type: KafkaReader

		def create_read_message_async_handle_thread_method():
			nonlocal kafka_reader

			kafka_reader = kafka_manager.get_reader(
				topic_name=topic_name,
				is_from_beginning=False
			).get_result()

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
				kafka_writer = kafka_manager.get_transactional_writer().get_result()  # type: KafkaTransactionalWriter
				write_message_async_handle = kafka_writer.write_message(
					topic_name=topic_name,
					message_bytes=f"message #{index}".encode()
				)
				kafka_writer.end_write_transaction()
				expected_written_message_async_handles.append(write_message_async_handle)

			for expected_written_message_async_handle in expected_written_message_async_handles:
				expected_written_message = expected_written_message_async_handle.get_result().get_message_bytes()
				print(f"written_message: {expected_written_message}")
				expected_messages_semaphore.acquire()
				expected_messages.append(expected_written_message)
				expected_messages_semaphore.release()

		read_messages = []  # type: List[bytes]
		read_messages_semaphore = Semaphore()

		def read_thread_method():
			nonlocal read_messages
			nonlocal read_messages_semaphore

			print("reading")
			kafka_reader_async_handle = kafka_reader.read_message()
			print(f"getting result")
			read_message = kafka_reader_async_handle.get_result().get_message_bytes()
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

		kafka_manager = get_default_kafka_manager()

		topic_name = str(uuid.uuid4())

		print(f"{datetime.utcnow()}: topic_name: {topic_name}")

		kafka_manager.add_topic(
			topic_name=topic_name
		).get_result()

		unexpected_message = b"unexpected"
		unexpected_written_message = None  # type: bytes

		def write_unexpected_thread_method():
			nonlocal unexpected_message
			nonlocal unexpected_written_message

			kafka_writer = kafka_manager.get_async_writer().get_result()  # type: KafkaAsyncWriter

			write_message_async_handle = kafka_writer.write_message(
				topic_name=topic_name,
				message_bytes=unexpected_message
			)

			unexpected_written_message = write_message_async_handle.get_result().get_message_bytes()

			print(f"{datetime.utcnow()}: unexpected_written_message: {unexpected_written_message}")

		kafka_reader = None  # type: KafkaReader

		def create_read_message_async_handle_thread_method():
			nonlocal kafka_reader

			kafka_reader = kafka_manager.get_reader(
				topic_name=topic_name,
				is_from_beginning=False
			).get_result()

			print(f"{datetime.utcnow()}: created reader")

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
				kafka_writer = kafka_manager.get_async_writer().get_result()  # type: KafkaAsyncWriter
				write_message_async_handle = kafka_writer.write_message(
					topic_name=topic_name,
					message_bytes=f"message #{index}".encode()
				)
				expected_written_message_async_handles.append(write_message_async_handle)

			for expected_written_message_async_handle in expected_written_message_async_handles:
				expected_written_message = expected_written_message_async_handle.get_result().get_message_bytes()
				print(f"{datetime.utcnow()}: written_message: {expected_written_message}")
				expected_messages_semaphore.acquire()
				expected_messages.append(expected_written_message)
				expected_messages_semaphore.release()

		read_messages = []  # type: List[bytes]
		read_messages_semaphore = Semaphore()

		def read_thread_method():
			nonlocal read_messages
			nonlocal read_messages_semaphore

			print(f"{datetime.utcnow()}: reading")
			kafka_reader_async_handle = kafka_reader.read_message()
			print(f"{datetime.utcnow()}: getting result")
			read_message = kafka_reader_async_handle.get_result().get_message_bytes()
			print(f"{datetime.utcnow()}: read message: {read_message}")
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

		time.sleep(1)

	def test_async_handle_wait(self):

		kafka_manager = get_default_kafka_manager()

		topic_name = str(uuid.uuid4())

		add_topic_async_handle = kafka_manager.add_topic(
			topic_name=topic_name
		)

		added_topic_name = add_topic_async_handle.get_result()

		self.assertEqual(topic_name, added_topic_name)

		topics = kafka_manager.get_topics().get_result()

		self.assertEqual((topic_name,), topics)

		print(f"test_async_handle_wait: {datetime.utcnow()}")
		start_time = datetime.utcnow()
		remove_topic_async_handle = kafka_manager.remove_topic(
			topic_name=topic_name
		)
		end_time = datetime.utcnow()
		print(f"test_async_handle_wait: remove_topic took {(end_time - start_time).total_seconds()} seconds")

		print(f"test_async_handle_wait: {datetime.utcnow()}")
		start_time = datetime.utcnow()
		is_successful = remove_topic_async_handle.try_wait(
			timeout_seconds=0.01
		)
		end_time = datetime.utcnow()
		print(f"test_async_handle_wait: wait took {(end_time - start_time).total_seconds()} seconds and is_successful: {is_successful}")

		self.assertFalse(is_successful)

		print(f"test_async_handle_wait: {datetime.utcnow()}")
		start_time = datetime.utcnow()
		is_successful = remove_topic_async_handle.try_wait(
			timeout_seconds=1
		)
		end_time = datetime.utcnow()
		actual_wait_seconds_total = (end_time - start_time).total_seconds()
		print(f"test_async_handle_wait: wait took {(end_time - start_time).total_seconds()} seconds and is_successful: {is_successful}")

		self.assertTrue(is_successful)

		print(f"test_async_handle_wait: {datetime.utcnow()}")
		start_time = datetime.utcnow()
		is_successful = remove_topic_async_handle.try_wait(
			timeout_seconds=0.01
		)
		end_time = datetime.utcnow()
		shorter_wait_seconds_total = (end_time - start_time).total_seconds()
		print(f"test_async_handle_wait: wait took {(end_time - start_time).total_seconds()} seconds and is_successful: {is_successful}")

		self.assertTrue(is_successful)
		self.assertLess(shorter_wait_seconds_total, actual_wait_seconds_total)

		print(f"test_async_handle_wait: {datetime.utcnow()}")
		removed_topic_name = remove_topic_async_handle.get_result()

		self.assertEqual(topic_name, removed_topic_name)

		topics = kafka_manager.get_topics().get_result()

		self.assertEqual((), topics)

		time.sleep(1)

	def test_write_one_message_read_until_end(self):

		kafka_manager = get_default_kafka_manager()

		topic_name = str(uuid.uuid4())

		kafka_manager.add_topic(
			topic_name=topic_name
		).get_result()

		kafka_writer = kafka_manager.get_async_writer().get_result()  # type: KafkaAsyncWriter

		kafka_writer.write_message(
			topic_name=topic_name,
			message_bytes=b"test"
		).get_result()

		time.sleep(10)

		kafka_reader = kafka_manager.get_reader(
			topic_name=topic_name,
			is_from_beginning=True
		).get_result()

		first_message = kafka_reader.try_read_message(timeout_seconds=4).get_result().get_message_bytes()

		self.assertIsNotNone(first_message)
		self.assertEqual(b"test", first_message)

		start_time = datetime.utcnow()
		second_message_quickly_discovered_empty_topic = kafka_reader.try_read_message(timeout_seconds=4).try_wait(
			timeout_seconds=6
		)
		end_time = datetime.utcnow()
		self.assertLess((end_time - start_time).total_seconds(), 5)

		self.assertTrue(second_message_quickly_discovered_empty_topic)

		second_message_async_handle = kafka_reader.try_read_message(timeout_seconds=4)

		second_message = second_message_async_handle.get_result()

		self.assertIsNone(second_message)
		self.assertFalse(second_message_async_handle.is_cancelled())

		time.sleep(1)

	def test_write_and_read_from_topic_async_everything_async(self):

		kafka_manager = get_default_kafka_manager()

		topic_name = str(uuid.uuid4())

		print(f"{datetime.utcnow()}: topic_name: {topic_name}")

		kafka_manager.add_topic(
			topic_name=topic_name
		).get_result()

		unexpected_message = b"unexpected"
		unexpected_written_message = None

		def write_unexpected_thread_method():
			nonlocal unexpected_message
			nonlocal unexpected_written_message

			kafka_writer = kafka_manager.get_async_writer().get_result()  # type: KafkaAsyncWriter

			write_message_async_handle = kafka_writer.write_message(
				topic_name=topic_name,
				message_bytes=unexpected_message
			)

			unexpected_written_message = write_message_async_handle.get_result().get_message_bytes()

			print(f"{datetime.utcnow()}: unexpected_written_message: {unexpected_written_message}")

		kafka_reader = None  # type: KafkaReader

		def create_read_message_async_handle_thread_method():
			nonlocal kafka_reader

			kafka_reader = kafka_manager.get_reader(
				topic_name=topic_name,
				is_from_beginning=True
			).get_result()

			print(f"{datetime.utcnow()}: created reader")

		expected_written_message = None
		expected_written_message_async_handles = []  # type: List[AsyncHandle]
		expected_messages = []  # type: List[bytes]
		expected_messages.append(unexpected_message)  # grab everything
		expected_messages_semaphore = Semaphore()

		def write_expected_thread_method():
			nonlocal expected_written_message
			nonlocal expected_written_message_async_handles
			nonlocal expected_messages
			nonlocal expected_messages_semaphore

			for index in range(10):
				kafka_writer = kafka_manager.get_async_writer().get_result()  # type: KafkaAsyncWriter
				write_message_async_handle = kafka_writer.write_message(
					topic_name=topic_name,
					message_bytes=f"message #{index}".encode()
				)
				expected_written_message_async_handles.append(write_message_async_handle)

			for expected_written_message_async_handle in expected_written_message_async_handles:
				expected_written_message = expected_written_message_async_handle.get_result().get_message_bytes()
				print(f"{datetime.utcnow()}: written_message: {expected_written_message}")
				expected_messages_semaphore.acquire()
				expected_messages.append(expected_written_message)
				expected_messages_semaphore.release()

		read_messages = []  # type: List[bytes]
		read_messages_semaphore = Semaphore()

		def read_thread_method():
			nonlocal read_messages
			nonlocal read_messages_semaphore

			print(f"{datetime.utcnow()}: reading")
			kafka_reader_async_handle = kafka_reader.read_message()
			print(f"{datetime.utcnow()}: getting result")
			read_message = kafka_reader_async_handle.get_result().get_message_bytes()
			print(f"{datetime.utcnow()}: read message: {read_message}")
			read_messages_semaphore.acquire()
			read_messages.append(read_message)
			read_messages_semaphore.release()

		write_unexpected_thread = start_thread(write_unexpected_thread_method)

		write_unexpected_thread.join()

		create_read_message_async_handle_thread = start_thread(create_read_message_async_handle_thread_method)

		create_read_message_async_handle_thread.join()

		read_threads = []
		for index in range(11):
			read_thread = start_thread(read_thread_method)
			read_threads.append(read_thread)

		# wait for the readers to have a change to poll, causing them to be assigned
		#time.sleep(10)

		write_expected_thread = start_thread(write_expected_thread_method)

		write_expected_thread.join()

		for read_thread in read_threads:
			read_thread.join()

		self.assertEqual(unexpected_message, unexpected_written_message)
		for expected_message in expected_messages:
			self.assertIn(expected_message, read_messages)
		for read_message in read_messages:
			self.assertIn(read_message, expected_messages)

		time.sleep(1)

	def test_write_and_read_from_topic_async_everything_sync(self):

		messages_total = 1000

		kafka_manager = get_default_kafka_manager()

		topic_name = str(uuid.uuid4())

		print(f"{datetime.utcnow()}: topic_name: {topic_name}")

		kafka_manager.add_topic(
			topic_name=topic_name
		).get_result()

		kafka_reader = None  # type: KafkaReader

		def create_read_message_async_handle_thread_method():
			nonlocal kafka_reader

			kafka_reader = kafka_manager.get_reader(
				topic_name=topic_name,
				is_from_beginning=True
			).get_result()

			print(f"{datetime.utcnow()}: created reader")

		expected_written_message = None
		expected_written_message_async_handles = []  # type: List[AsyncHandle]
		expected_messages = []  # type: List[bytes]
		expected_messages_semaphore = Semaphore()

		def write_expected_thread_method():
			nonlocal expected_written_message
			nonlocal expected_written_message_async_handles
			nonlocal expected_messages
			nonlocal expected_messages_semaphore
			nonlocal messages_total

			kafka_writer = kafka_manager.get_async_writer().get_result()  # type: KafkaAsyncWriter
			for index in range(messages_total):
				write_message_async_handle = kafka_writer.write_message(
					topic_name=topic_name,
					message_bytes=f"message #{index}".encode()
				)
				expected_written_message_async_handles.append(write_message_async_handle)

			for expected_written_message_async_handle in expected_written_message_async_handles:
				expected_written_message = expected_written_message_async_handle.get_result().get_message_bytes()
				print(f"{datetime.utcnow()}: written_message: {expected_written_message}")
				expected_messages_semaphore.acquire()
				expected_messages.append(expected_written_message)
				expected_messages_semaphore.release()

		read_messages = []  # type: List[bytes]
		read_messages_semaphore = Semaphore()
		synchronized_read_semaphore = Semaphore()

		def read_thread_method():
			nonlocal read_messages
			nonlocal read_messages_semaphore
			nonlocal synchronized_read_semaphore

			synchronized_read_semaphore.acquire()

			print(f"{datetime.utcnow()}: reading")
			kafka_reader_async_handle = kafka_reader.read_message()
			print(f"{datetime.utcnow()}: getting result")
			read_message = kafka_reader_async_handle.get_result().get_message_bytes()
			print(f"{datetime.utcnow()}: read message: {read_message}")
			read_messages_semaphore.acquire()
			read_messages.append(read_message)
			read_messages_semaphore.release()

			synchronized_read_semaphore.release()

		create_read_message_async_handle_thread = start_thread(create_read_message_async_handle_thread_method)

		create_read_message_async_handle_thread.join()

		read_threads = []
		for index in range(messages_total):
			read_thread = start_thread(read_thread_method)
			read_threads.append(read_thread)

		# wait for the readers to have a change to poll, causing them to be assigned
		#time.sleep(10)

		write_expected_thread = start_thread(write_expected_thread_method)

		write_expected_thread.join()

		for read_thread in read_threads:
			read_thread.join()

		for expected_message in expected_messages:
			self.assertIn(expected_message, read_messages)
		for read_message in read_messages:
			self.assertIn(read_message, expected_messages)

		time.sleep(1)

	def test_read_first_and_read_last(self):

		kafka_manager = get_default_kafka_manager()

		kafka_writer = kafka_manager.get_async_writer().get_result()  # type: KafkaAsyncWriter

		topic_name = str(uuid.uuid4())

		kafka_manager.add_topic(
			topic_name=topic_name
		).get_result()

		kafka_writer.write_message(
			topic_name=topic_name,
			message_bytes=b"first"
		).get_result()

		kafka_writer.write_message(
			topic_name=topic_name,
			message_bytes=b"second"
		).get_result()

		kafka_writer.write_message(
			topic_name=topic_name,
			message_bytes=b"third"
		).get_result()

		kafka_reader = kafka_manager.get_reader(
			topic_name=topic_name,
			is_from_beginning=True
		).get_result()  # type: KafkaReader

		first_message = kafka_reader.read_message().get_result()  # type: KafkaMessage

		print(f"first_message: {first_message}")
		self.assertEqual(b"first", first_message.get_message_bytes())

		seek_index = kafka_reader.get_seek_index().get_result()  # type: KafkaPartitionSeekIndex

		print(f"seek_index.get_partition_indexes(): {seek_index.get_partition_indexes()}")
		self.assertEqual((1,), seek_index.get_partition_indexes())

		kafka_reader.set_seek_index_to_end().get_result()

		time.sleep(0.5)  # TODO since the queued.min.messages (not being 1 due to inefficiencies) is probably pulling in the next message, this delay is necessary

		kafka_writer.write_message(
			topic_name=topic_name,
			message_bytes=b"fourth"
		).get_result()

		fourth_message = kafka_reader.read_message().get_result()  # type: KafkaMessage

		print(f"fourth_message: {fourth_message}")
		self.assertEqual(b"fourth", fourth_message.get_message_bytes())

		time.sleep(1)

	def test_read_message_then_reset_to_message_seek_index_and_read_again(self):

		kafka_manager = get_default_kafka_manager()

		topic_name = str(uuid.uuid4())

		kafka_manager.add_topic(
			topic_name=topic_name
		).get_result()

		for index in range(100):
			kafka_manager.get_async_writer().get_result().write_message(
				topic_name=topic_name,
				message_bytes=b"left side"
			).get_result()

		written_kafka_message = kafka_manager.get_async_writer().get_result().write_message(
			topic_name=topic_name,
			message_bytes=b"test"
		).get_result()  # type: KafkaMessage

		for index in range(100):
			kafka_manager.get_async_writer().get_result().write_message(
				topic_name=topic_name,
				message_bytes=b"right side"
			).get_result()

		kafka_reader = kafka_manager.get_reader(
			topic_name=topic_name,
			is_from_beginning=True
		).get_result()  # type: KafkaReader

		kafka_reader.set_seek_index(
			kafka_topic_seek_index=kafka_manager.get_kafka_topic_seek_index_from_kafka_message(
				kafka_message=written_kafka_message
			).get_result()
		)

		read_message = kafka_reader.read_message().get_result()  # type: KafkaMessage

		self.assertEqual(written_kafka_message.get_message_bytes(), read_message.get_message_bytes())
		self.assertEqual(written_kafka_message.get_topic_name(), read_message.get_topic_name())
		self.assertEqual(written_kafka_message.get_offset(), read_message.get_offset())
		self.assertEqual(written_kafka_message.get_partition_index(), read_message.get_partition_index())

		time.sleep(1)

	def test_read_write_efficiency_sequential_sync(self):

		kafka_manager = get_default_kafka_manager()

		topic_name = str(uuid.uuid4())

		kafka_manager.add_topic(
			topic_name=topic_name
		).get_result()

		kafka_reader = kafka_manager.get_reader(
			topic_name=topic_name,
			is_from_beginning=True
		).get_result()  # type: KafkaReader

		kafka_writer = kafka_manager.get_async_writer().get_result()  # type: KafkaAsyncWriter

		trials = []  # type: List[float]
		writes = []  # type: List[float]
		reads = []  # type: List[float]
		for index in range(20):
			start_time = datetime.utcnow()
			kafka_writer.write_message(
				topic_name=topic_name,
				message_bytes=f"i:{index}".encode()
			).get_result()
			write_time = datetime.utcnow()
			kafka_reader.read_message().get_result()
			end_time = datetime.utcnow()
			trials.append((end_time - start_time).total_seconds())
			writes.append((write_time - start_time).total_seconds())
			reads.append((end_time - write_time).total_seconds())

		#print(f"Trials: {trials}")
		print("Totals:")
		print(f"Max: {max(trials)} at {trials.index(max(trials))} which is {1.0/max(trials)} in a second")
		print(f"Min: {min(trials)} at {trials.index(min(trials))} which is {1.0/min(trials)} in a second")
		trials.remove(max(trials))
		trials.remove(min(trials))
		print(f"Ave: {sum(trials)/len(trials)} which is {1.0/(sum(trials)/len(trials))} in a second")
		print("Writes:")
		print(f"Max: {max(writes)} at {writes.index(max(writes))} which is {1.0/max(writes)} in a second")
		print(f"Min: {min(writes)} at {writes.index(min(writes))} which is {1.0/min(writes)} in a second")
		writes.remove(max(writes))
		writes.remove(min(writes))
		print(f"Ave: {sum(writes)/len(writes)} which is {1.0/(sum(writes)/len(writes))} in a second")
		print("Reads:")
		print(f"Max: {max(reads)} at {reads.index(max(reads))} which is {1.0/max(reads)} in a second")
		print(f"Min: {min(reads)} at {reads.index(min(reads))} which is {1.0/min(reads)} in a second")
		reads.remove(max(reads))
		reads.remove(min(reads))
		print(f"Ave: {sum(reads)/len(reads)} which is {1.0/(sum(reads)/len(reads))} in a second")

		time.sleep(1)

	def test_read_write_efficiency_write_then_read(self):

		kafka_manager = get_default_kafka_manager()

		topic_name = str(uuid.uuid4())

		kafka_manager.add_topic(
			topic_name=topic_name
		).get_result()

		kafka_reader = kafka_manager.get_reader(
			topic_name=topic_name,
			is_from_beginning=True
		).get_result()  # type: KafkaReader

		kafka_writer = kafka_manager.get_async_writer().get_result()  # type: KafkaAsyncWriter

		writes = []  # type: List[float]
		for index in range(20):
			start_time = datetime.utcnow()
			kafka_writer.write_message(
				topic_name=topic_name,
				message_bytes=f"i:{index}".encode()
			).get_result()
			end_time = datetime.utcnow()
			writes.append((end_time - start_time).total_seconds())

		reads = []  # type: List[float]
		for index in range(20):
			start_time = datetime.utcnow()
			kafka_reader.read_message().get_result()
			end_time = datetime.utcnow()
			reads.append((end_time - start_time).total_seconds())

		print("Writes:")
		print(f"Max: {max(writes)} at {writes.index(max(writes))} which is {1.0/max(writes)} in a second")
		print(f"Min: {min(writes)} at {writes.index(min(writes))} which is {1.0/min(writes)} in a second")
		writes.remove(max(writes))
		writes.remove(min(writes))
		print(f"Ave: {sum(writes)/len(writes)} which is {1.0/(sum(writes)/len(writes))} in a second")
		print("Reads:")
		print(f"Max: {max(reads)} at {reads.index(max(reads))} which is {1.0/max(reads)} in a second")
		print(f"Min: {min(reads)} at {reads.index(min(reads))} which is {1.0/min(reads)} in a second")
		reads.remove(max(reads))
		reads.remove(min(reads))
		print(f"Ave: {sum(reads)/len(reads)} which is {1.0/(sum(reads)/len(reads))} in a second")

		time.sleep(1)

	def test_read_write_efficiency_1KB(self):

		kafka_manager = get_default_kafka_manager()

		topic_name = str(uuid.uuid4())

		kafka_manager.add_topic(
			topic_name=topic_name
		).get_result()

		kafka_reader = kafka_manager.get_reader(
			topic_name=topic_name,
			is_from_beginning=True
		).get_result()  # type: KafkaReader

		kafka_writer = kafka_manager.get_async_writer().get_result()  # type: KafkaAsyncWriter

		trials = []  # type: List[float]
		kilobyte_message_bytes = b"12345678" * 128
		for index in range(100):
			start_time = datetime.utcnow()
			kafka_writer.write_message(
				topic_name=topic_name,
				message_bytes=kilobyte_message_bytes
			).get_result()
			kafka_reader.read_message().get_result()
			end_time = datetime.utcnow()
			trials.append((end_time - start_time).total_seconds())

		#print(f"Trials: {trials}")
		print(f"Max: {max(trials)} at {trials.index(max(trials))} which is {1.0/max(trials)} in a second")
		print(f"Min: {min(trials)} at {trials.index(min(trials))} which is {1.0/min(trials)} in a second")
		print(f"Ave: {sum(trials)/len(trials)} which is {1.0/(sum(trials)/len(trials))} in a second")

		time.sleep(1)

	def test_read_write_efficiency_10KB(self):

		kafka_manager = get_default_kafka_manager()

		topic_name = str(uuid.uuid4())

		kafka_manager.add_topic(
			topic_name=topic_name
		).get_result()

		kafka_reader = kafka_manager.get_reader(
			topic_name=topic_name,
			is_from_beginning=True
		).get_result()  # type: KafkaReader

		kafka_writer = kafka_manager.get_async_writer().get_result()  # type: KafkaAsyncWriter

		trials = []  # type: List[float]
		kilobyte_message_bytes = b"12345678" * 128 * 10
		for index in range(100):
			start_time = datetime.utcnow()
			kafka_writer.write_message(
				topic_name=topic_name,
				message_bytes=kilobyte_message_bytes
			).get_result()
			kafka_reader.read_message().get_result()
			end_time = datetime.utcnow()
			trials.append((end_time - start_time).total_seconds())

		#print(f"Trials: {trials}")
		print(f"Max: {max(trials)} at {trials.index(max(trials))} which is {1.0/max(trials)} in a second")
		print(f"Min: {min(trials)} at {trials.index(min(trials))} which is {1.0/min(trials)} in a second")
		print(f"Ave: {sum(trials)/len(trials)} which is {1.0/(sum(trials)/len(trials))} in a second")

		time.sleep(1)

	def test_parallel_write_and_read_efficiency(self):

		read_write_total = 1000

		kafka_manager = get_default_kafka_manager()

		topic_name = str(uuid.uuid4())

		kafka_manager.add_topic(
			topic_name=topic_name
		).get_result()

		kafka_writer = kafka_manager.get_async_writer().get_result()  # type: KafkaAsyncWriter

		kafka_reader = kafka_manager.get_reader(
			topic_name=topic_name,
			is_from_beginning=True
		).get_result()  # type: KafkaReader

		block_semaphore = Semaphore()
		block_semaphore.acquire()

		write_start_times = []  # type: List[datetime]
		write_end_times = []  # type: List[datetime]

		read_start_times = []  # type: List[datetime]
		read_end_times = []  # type: List[datetime]

		expected_message_bytes = b"test"

		def write_thread_method():
			nonlocal block_semaphore
			nonlocal kafka_writer
			nonlocal expected_message_bytes
			nonlocal read_write_total

			block_semaphore.acquire()
			block_semaphore.release()

			for index in range(read_write_total):
				write_start_times.append(datetime.utcnow())
				kafka_writer.write_message(
					topic_name=topic_name,
					message_bytes=expected_message_bytes
				).get_result()
				write_end_times.append(datetime.utcnow())

		def read_thread_method():
			nonlocal block_semaphore
			nonlocal kafka_reader
			nonlocal read_write_total

			block_semaphore.acquire()
			block_semaphore.release()

			for index in range(read_write_total):
				read_start_times.append(datetime.utcnow())
				kafka_reader.read_message().get_result()
				read_end_times.append(datetime.utcnow())

		write_thread = start_thread(write_thread_method)
		read_thread = start_thread(read_thread_method)

		time.sleep(1.0)

		block_semaphore.release()

		write_thread.join()
		read_thread.join()

		write_time_seconds = []  # type: List[float]
		read_time_seconds = []  # type: List[float]
		write_start_to_read_end_time_seconds = []  # type: List[float]
		for index in range(read_write_total):
			write_time = (write_end_times[index] - write_start_times[index]).total_seconds()
			read_time = (read_end_times[index] - read_start_times[index]).total_seconds()
			write_start_to_read_end_time = (read_end_times[index] - write_start_times[index]).total_seconds()

			write_time_seconds.append(write_time)
			read_time_seconds.append(read_time)
			write_start_to_read_end_time_seconds.append(write_start_to_read_end_time)

		print(f"Writes:")
		print(f"Max: {max(write_time_seconds)} at {write_time_seconds.index(max(write_time_seconds))} which is {1.0 / max(write_time_seconds)} in a second")
		print(f"Min: {min(write_time_seconds)} at {write_time_seconds.index(min(write_time_seconds))} which is {1.0 / min(write_time_seconds)} in a second")
		print(f"Ave: {sum(write_time_seconds) / len(write_time_seconds)} which is {1.0 / (sum(write_time_seconds) / len(write_time_seconds))} in a second")

		print(f"Reads:")
		print(f"Max: {max(read_time_seconds)} at {read_time_seconds.index(max(read_time_seconds))} which is {1.0 / max(read_time_seconds)} in a second")
		print(f"Min: {min(read_time_seconds)} at {read_time_seconds.index(min(read_time_seconds))} which is {1.0 / min(read_time_seconds)} in a second")
		print(f"Ave: {sum(read_time_seconds) / len(read_time_seconds)} which is {1.0 / (sum(read_time_seconds) / len(read_time_seconds))} in a second")

		print(f"Write-to-Read:")
		print(f"Max: {max(write_start_to_read_end_time_seconds)} at {write_start_to_read_end_time_seconds.index(max(write_start_to_read_end_time_seconds))} which is {1.0 / max(write_start_to_read_end_time_seconds)} in a second")
		print(f"Min: {min(write_start_to_read_end_time_seconds)} at {write_start_to_read_end_time_seconds.index(min(write_start_to_read_end_time_seconds))} which is {1.0 / min(write_start_to_read_end_time_seconds)} in a second")
		print(f"Ave: {sum(write_start_to_read_end_time_seconds) / len(write_start_to_read_end_time_seconds)} which is {1.0 / (sum(write_start_to_read_end_time_seconds) / len(write_start_to_read_end_time_seconds))} in a second")

		if True:
			write_time_seconds.remove(max(write_time_seconds))
			plt.scatter(write_time_seconds, list(range(len(write_time_seconds))), c="blue", s=1)
			plt.scatter(read_time_seconds, list(range(len(read_time_seconds))), c="red", s=1)
			plt.scatter(write_start_to_read_end_time_seconds, list(range(len(write_start_to_read_end_time_seconds))), c="purple", s=1)
			#plt.gcf().autofmt_xdate()
			plt.show()

			plt.scatter(write_start_times, list(range(len(write_start_times))), c="blue", s=1)
			plt.scatter(write_end_times, list(range(len(write_end_times))), c="green", s=1)
			plt.scatter(read_start_times, list(range(len(read_start_times))), c="red", s=1)
			plt.scatter(read_end_times, list(range(len(read_end_times))), c="purple", s=1)
			plt.gcf().autofmt_xdate()
			plt.show()

		time.sleep(1)

	def test_parallel_write_and_read_efficiency_multiple_writers(self):

		read_write_total = 1000
		unexpected_writers_total = 50

		kafka_manager = get_default_kafka_manager()

		topic_name = str(uuid.uuid4())

		kafka_manager.add_topic(
			topic_name=topic_name
		).get_result()

		kafka_writer = kafka_manager.get_async_writer().get_result()  # type: KafkaAsyncWriter

		kafka_reader = kafka_manager.get_reader(
			topic_name=topic_name,
			is_from_beginning=True
		).get_result()  # type: KafkaReader

		block_semaphore = Semaphore()
		block_semaphore.acquire()

		write_start_times = []  # type: List[datetime]
		write_end_times = []  # type: List[datetime]

		read_start_times = []  # type: List[datetime]
		read_end_times = []  # type: List[datetime]

		expected_message_bytes = b"test"
		unexpected_message_bytes = b"foobar"

		def write_thread_method():
			nonlocal block_semaphore
			nonlocal kafka_writer
			nonlocal expected_message_bytes
			nonlocal read_write_total

			block_semaphore.acquire()
			block_semaphore.release()

			for index in range(read_write_total):
				write_start_times.append(datetime.utcnow())
				kafka_writer.write_message(
					topic_name=topic_name,
					message_bytes=expected_message_bytes
				).get_result()
				write_end_times.append(datetime.utcnow())

		def unexpected_write_thread_method():
			nonlocal block_semaphore
			nonlocal unexpected_message_bytes
			nonlocal read_write_total
			nonlocal kafka_manager

			unexpected_kafka_writer = kafka_manager.get_async_writer().get_result()  # type: KafkaAsyncWriter

			block_semaphore.acquire()
			block_semaphore.release()

			for index in range(read_write_total):
				unexpected_kafka_writer.write_message(
					topic_name=topic_name,
					message_bytes=unexpected_message_bytes
				).get_result()

		def read_thread_method():
			nonlocal block_semaphore
			nonlocal kafka_reader
			nonlocal read_write_total
			nonlocal unexpected_writers_total

			block_semaphore.acquire()
			block_semaphore.release()

			for index in range(read_write_total * (unexpected_writers_total + 1)):
				read_start_time = datetime.utcnow()
				kafka_message = kafka_reader.read_message().get_result()  # type: KafkaMessage
				read_end_time = datetime.utcnow()
				if kafka_message.get_message_bytes() == expected_message_bytes:
					read_start_times.append(read_start_time)
					read_end_times.append(read_end_time)

		write_thread = start_thread(write_thread_method)
		read_thread = start_thread(read_thread_method)
		unexpected_write_threads = []
		for index in range(unexpected_writers_total):
			unexpected_write_thread = start_thread(unexpected_write_thread_method)
			unexpected_write_threads.append(unexpected_write_thread)

		time.sleep(1.0)

		block_semaphore.release()

		write_thread.join()
		read_thread.join()
		for unexpected_write_thread in unexpected_write_threads:
			unexpected_write_thread.join()

		write_time_seconds = []  # type: List[float]
		read_time_seconds = []  # type: List[float]
		write_start_to_read_end_time_seconds = []  # type: List[float]
		for index in range(read_write_total):
			write_time = (write_end_times[index] - write_start_times[index]).total_seconds()
			read_time = (read_end_times[index] - read_start_times[index]).total_seconds()
			write_start_to_read_end_time = (read_end_times[index] - write_start_times[index]).total_seconds()

			write_time_seconds.append(write_time)
			read_time_seconds.append(read_time)
			write_start_to_read_end_time_seconds.append(write_start_to_read_end_time)

		print(f"Writes:")
		print(f"Max: {max(write_time_seconds)} at {write_time_seconds.index(max(write_time_seconds))} which is {1.0 / max(write_time_seconds)} in a second")
		print(f"Min: {min(write_time_seconds)} at {write_time_seconds.index(min(write_time_seconds))} which is {1.0 / min(write_time_seconds)} in a second")
		print(f"Ave: {sum(write_time_seconds) / len(write_time_seconds)} which is {1.0 / (sum(write_time_seconds) / len(write_time_seconds))} in a second")

		print(f"Reads:")
		print(f"Max: {max(read_time_seconds)} at {read_time_seconds.index(max(read_time_seconds))} which is {1.0 / max(read_time_seconds)} in a second")
		print(f"Min: {min(read_time_seconds)} at {read_time_seconds.index(min(read_time_seconds))} which is {1.0 / min(read_time_seconds)} in a second")
		print(f"Ave: {sum(read_time_seconds) / len(read_time_seconds)} which is {1.0 / (sum(read_time_seconds) / len(read_time_seconds))} in a second")

		print(f"Write-to-Read:")
		print(f"Max: {max(write_start_to_read_end_time_seconds)} at {write_start_to_read_end_time_seconds.index(max(write_start_to_read_end_time_seconds))} which is {1.0 / max(write_start_to_read_end_time_seconds)} in a second")
		print(f"Min: {min(write_start_to_read_end_time_seconds)} at {write_start_to_read_end_time_seconds.index(min(write_start_to_read_end_time_seconds))} which is {1.0 / min(write_start_to_read_end_time_seconds)} in a second")
		print(f"Ave: {sum(write_start_to_read_end_time_seconds) / len(write_start_to_read_end_time_seconds)} which is {1.0 / (sum(write_start_to_read_end_time_seconds) / len(write_start_to_read_end_time_seconds))} in a second")

		time.sleep(1)

	def test_read_from_topic_before_writing(self):

		kafka_manager = KafkaManager(
			kafka_wrapper=KafkaWrapper(
				host_pointer=HostPointer(
					host_address="0.0.0.0",
					host_port=9092
				)
			),
			read_polling_seconds=0.1,
			is_cancelled_polling_seconds=0.1,
			new_topic_partitions_total=1,
			new_topic_replication_factor=1,
			remove_topic_cluster_propagation_blocking_timeout_seconds=30
		)

		kafka_topic_name = str(uuid.uuid4())

		kafka_manager.add_topic(
			topic_name=kafka_topic_name
		).get_result()

		kafka_reader = kafka_manager.get_reader(
			topic_name=kafka_topic_name,
			is_from_beginning=True
		).get_result()  # type: KafkaReader

		kafka_writer = kafka_manager.get_async_writer().get_result()  # type: KafkaAsyncWriter
		def read_thread_method():
			nonlocal kafka_reader
			for index in range(10):
				message = kafka_reader.read_message().get_result()  # type: KafkaMessage
				self.assertEqual(str(index).encode(), message.get_message_bytes())

		def write_thread_method():
			nonlocal kafka_writer
			for index in range(10):
				kafka_writer.write_message(
					topic_name=kafka_topic_name,
					message_bytes=str(index).encode()
				).get_result()

		read_thread = start_thread(read_thread_method)

		time.sleep(1)

		write_thread = start_thread(write_thread_method)
		write_thread.join()

		read_thread.join()

		time.sleep(1)

	def test_read_from_topic_after_writing(self):

		kafka_manager = KafkaManager(
			kafka_wrapper=KafkaWrapper(
				host_pointer=HostPointer(
					host_address="0.0.0.0",
					host_port=9092
				)
			),
			read_polling_seconds=0.1,
			is_cancelled_polling_seconds=0.1,
			new_topic_partitions_total=1,
			new_topic_replication_factor=1,
			remove_topic_cluster_propagation_blocking_timeout_seconds=30
		)

		kafka_topic_name = str(uuid.uuid4())

		kafka_manager.add_topic(
			topic_name=kafka_topic_name
		).get_result()

		kafka_reader = kafka_manager.get_reader(
			topic_name=kafka_topic_name,
			is_from_beginning=True
		).get_result()  # type: KafkaReader

		kafka_writer = kafka_manager.get_async_writer().get_result()  # type: KafkaAsyncWriter
		def read_thread_method():
			nonlocal kafka_reader
			for index in range(10):
				message = kafka_reader.read_message().get_result()  # type: KafkaMessage
				self.assertEqual(str(index).encode(), message.get_message_bytes())

		def write_thread_method():
			nonlocal kafka_writer
			for index in range(10):
				kafka_writer.write_message(
					topic_name=kafka_topic_name,
					message_bytes=str(index).encode()
				).get_result()

		write_thread = start_thread(write_thread_method)
		write_thread.join()

		read_thread = start_thread(read_thread_method)
		read_thread.join()

		time.sleep(1)
