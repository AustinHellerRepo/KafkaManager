import unittest
from src.austin_heller_repo.kafka_manager import KafkaManager, KafkaReader, KafkaWrapper, KafkaTopicSeekIndex, KafkaMessage
from austin_heller_repo.threading import start_thread, Semaphore, BooleanReference, AsyncHandle
import uuid
import time
from datetime import datetime
from typing import List, Tuple, Dict


def get_default_kafka_manager() -> KafkaManager:

	kafka_wrapper = KafkaWrapper(
		host_url="0.0.0.0",
		host_port=9092
	)

	kafka_manager = KafkaManager(
		kafka_wrapper=kafka_wrapper,
		read_polling_seconds=1.0,
		is_cancelled_polling_seconds=0.01,
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

		kafka_manager = get_default_kafka_manager()

		self.assertIsNotNone(kafka_manager)

	def test_list_all_topics(self):

		kafka_manager = get_default_kafka_manager()

		topics = kafka_manager.get_topics()

		print(f"topics: {topics}")

		self.assertEqual((), topics)

	def test_add_and_remove_topic(self):

		kafka_manager = get_default_kafka_manager()

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

			kafka_writer = kafka_manager.get_transactional_writer()

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
				kafka_writer = kafka_manager.get_transactional_writer()
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

			kafka_writer = kafka_manager.get_async_writer()

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
				kafka_writer = kafka_manager.get_async_writer()
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

	def test_async_handle_wait(self):

		kafka_manager = get_default_kafka_manager()

		topic_name = str(uuid.uuid4())

		add_topic_async_handle = kafka_manager.add_topic(
			topic_name=topic_name
		)

		added_topic_name = add_topic_async_handle.get_result()

		self.assertEqual(topic_name, added_topic_name)

		topics = kafka_manager.get_topics()

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

		topics = kafka_manager.get_topics()

		self.assertEqual((), topics)

	def test_write_one_message_read_until_end(self):

		kafka_manager = get_default_kafka_manager()

		topic_name = str(uuid.uuid4())

		kafka_manager.add_topic(
			topic_name=topic_name
		).get_result()

		kafka_writer = kafka_manager.get_async_writer()

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

			kafka_writer = kafka_manager.get_async_writer()

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
				kafka_writer = kafka_manager.get_async_writer()
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

			for index in range(messages_total):
				kafka_writer = kafka_manager.get_async_writer()
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

	def test_read_first_and_read_last(self):

		kafka_manager = get_default_kafka_manager()

		kafka_writer = kafka_manager.get_async_writer()

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

		kafka_writer.write_message(
			topic_name=topic_name,
			message_bytes=b"fourth"
		).get_result()

		fourth_message = kafka_reader.read_message().get_result()  # type: KafkaMessage

		print(f"fourth_message: {fourth_message}")
		self.assertEqual(b"fourth", fourth_message.get_message_bytes())

	def test_read_message_then_reset_to_message_seek_index_and_read_again(self):

		kafka_manager = get_default_kafka_manager()

		topic_name = str(uuid.uuid4())

		kafka_manager.add_topic(
			topic_name=topic_name
		).get_result()

		for index in range(100):
			kafka_manager.get_async_writer().write_message(
				topic_name=topic_name,
				message_bytes=b"left side"
			).get_result()

		written_kafka_message = kafka_manager.get_async_writer().write_message(
			topic_name=topic_name,
			message_bytes=b"test"
		).get_result()  # type: KafkaMessage

		for index in range(100):
			kafka_manager.get_async_writer().write_message(
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
			)
		)

		read_message = kafka_reader.read_message().get_result()  # type: KafkaMessage

		self.assertEqual(written_kafka_message.get_message_bytes(), read_message.get_message_bytes())
		self.assertEqual(written_kafka_message.get_topic_name(), read_message.get_topic_name())
		self.assertEqual(written_kafka_message.get_offset(), read_message.get_offset())
		self.assertEqual(written_kafka_message.get_partition_index(), read_message.get_partition_index())

	def test_read_write_efficiency(self):

		kafka_manager = get_default_kafka_manager()

		topic_name = str(uuid.uuid4())

		kafka_manager.add_topic(
			topic_name=topic_name
		).get_result()

		kafka_reader = kafka_manager.get_reader(
			topic_name=topic_name,
			is_from_beginning=True
		).get_result()

		kafka_writer = kafka_manager.get_async_writer()

		trials = []  # type: List[float]
		for index in range(10000):
			start_time = datetime.utcnow()
			kafka_writer.write_message(
				topic_name=topic_name,
				message_bytes=f"index:{index}".encode()
			)
			kafka_reader.read_message()
			end_time = datetime.utcnow()
			trials.append((end_time - start_time).total_seconds())

		#print(f"Trials: {trials}")
		print(f"Max: {max(trials)} at {trials.index(max(trials))} which is {1.0/max(trials)} in a second")
		print(f"Min: {min(trials)} at {trials.index(min(trials))} which is {1.0/min(trials)} in a second")
		print(f"Ave: {sum(trials)/len(trials)} which is {1.0/(sum(trials)/len(trials))} in a second")
