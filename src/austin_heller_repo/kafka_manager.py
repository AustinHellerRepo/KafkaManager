from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import Producer, Consumer, Message, KafkaError, TopicPartition
from concurrent.futures import Future
from typing import List, Tuple, Dict, Callable
from austin_heller_repo.threading import Semaphore
import uuid


class AddTopicException(Exception):

	def __init__(self, *args):
		super().__init__(*args)


class RemoveTopicException(Exception):

	def __init__(self, *args):
		super().__init__(*args)


class BeginWriteException(Exception):

	def __init__(self, *args):
		super().__init__(*args)


class WriteMessageException(Exception):

	def __init__(self, *args):
		super().__init__(*args)


class ReadMessageException(Exception):

	def __init__(self, *args):
		super().__init__(*args)


class AsyncHandle():

	def __init__(self, *, get_result_method: Callable[[], object]):

		self.__get_result_method = get_result_method

	def get_result(self) -> object:

		return self.__get_result_method()


class KafkaAsyncWriter():

	def __init__(self, *, producer: Producer):

		self.__producer = producer

	def write_message(self, *, topic_name: str, partition_index: int, message_bytes: bytes) -> AsyncHandle:

		if not isinstance(message_bytes, bytes):
			raise WriteMessageException("message_bytes object must be of type \"bytes\".")

		callback_error = None  # type: KafkaError
		callback_message = None  # type: Message
		callback_semaphore = Semaphore()
		callback_semaphore.acquire()

		def get_result_method() -> bytes:
			nonlocal callback_error
			nonlocal callback_message
			nonlocal callback_semaphore

			self.__producer.flush()

			callback_semaphore.acquire()
			callback_semaphore.release()

			if callback_error is not None:
				raise WriteMessageException(callback_error)
			else:
				return callback_message.value()

		def callback(error: KafkaError, message: Message):
			nonlocal callback_error
			nonlocal callback_message
			nonlocal callback_semaphore

			callback_error = error
			callback_message = message

			if callback_error is None and callback_message.error() is not None:
				callback_error = callback_message.error()

			callback_semaphore.release()

		self.__producer.produce(topic_name, message_bytes, partition=partition_index, callback=callback)

		async_handle = AsyncHandle(
			get_result_method=get_result_method
		)

		return async_handle


class KafkaTransactionalWriter():

	def __init__(self, *, producer: Producer):

		self.__producer = producer

		self.__kafka_async_writer = KafkaAsyncWriter(
			producer=producer
		)

	def write_message(self, *, topic_name: str, partition_index: int, message_bytes: bytes) -> AsyncHandle:

		return self.__kafka_async_writer.write_message(
			topic_name=topic_name,
			partition_index=partition_index,
			message_bytes=message_bytes
		)

	def end_write_transaction(self):

		self.__producer.commit_transaction()


class KafkaReader():

	def __init__(self, *, consumer: Consumer, read_polling_seconds: float):

		self.__consumer = consumer
		self.__read_polling_seconds = read_polling_seconds

	def read_message(self) -> AsyncHandle:

		def get_result_method():

			message = None  # type: Message

			while message is None:
				message = self.__consumer.poll(self.__read_polling_seconds)
				if message is not None:
					message_error = message.error()
					if message_error is not None:
						raise ReadMessageException(message_error)

			return message.value()

		async_handle = AsyncHandle(
			get_result_method=get_result_method
		)

		return async_handle


class KafkaManager():

	def __init__(self, *, host_url: str, host_port: int, read_polling_seconds: float, cluster_propagation_seconds: float):

		self.__host_url = host_url
		self.__host_port = host_port
		self.__read_polling_seconds = read_polling_seconds
		self.__cluster_propagation_seconds = cluster_propagation_seconds

		self.__admin_client = None  # type: AdminClient
		self.__admin_client_semaphore = Semaphore()
		self.__async_producer = None  # type: Producer
		self.__async_producer_semaphore = Semaphore()
		self.__consumers = []  # type: List[Consumer]
		self.__consumers_semaphore = Semaphore()
		self.__write_transaction = None

	def __get_bootstrap_servers(self) -> str:
		return f"{self.__host_url}:{self.__host_port}"

	def __get_admin_client(self) -> AdminClient:

		self.__admin_client_semaphore.acquire()
		if self.__admin_client is None:
			self.__admin_client = AdminClient({
				"bootstrap.servers": self.__get_bootstrap_servers()
			})
		self.__admin_client_semaphore.release()
		return self.__admin_client

	def __get_producer(self, *, is_transactional: bool) -> Producer:

		if is_transactional:
			producer = Producer({
				"bootstrap.servers": self.__get_bootstrap_servers(),
				"transactional.id": str(uuid.uuid4())
			})
			producer.init_transactions()
			producer.begin_transaction()
		else:
			self.__async_producer_semaphore.acquire()
			if self.__async_producer is None:
				self.__async_producer = Producer({
					"bootstrap.servers": self.__get_bootstrap_servers()
				})
			self.__async_producer_semaphore.release()
			producer = self.__async_producer
		return producer

	def __get_consumer(self, *, topic_name: str, group_name: str, is_from_beginning: bool):

		if topic_name.startswith("^"):
			raise ReadMessageException(f"topic_name cannot start with \"^\" character.")
		self.__consumers_semaphore.acquire()
		consumer = Consumer({
			"bootstrap.servers": self.__get_bootstrap_servers(),
			"group.id": group_name,
			"auto.offset.reset": "earliest" if is_from_beginning else "latest"
		})
		consumer.subscribe([topic_name])
		self.__consumers.append(consumer)
		self.__consumers_semaphore.release()
		return consumer

	def __wait_for_cluster_propagation(self):

		admin_client = self.__get_admin_client()
		admin_client.poll(self.__cluster_propagation_seconds)

	def add_topic(self, *, topic_name: str, partition_total: int, replication_factor: int) -> AsyncHandle:

		if topic_name.startswith("__"):
			raise AddTopicException("topic_name cannot start with \"__\".")
		else:

			admin_client = self.__get_admin_client()

			topic = NewTopic(topic_name, partition_total, replication_factor)

			future_per_topic = admin_client.create_topics([topic])  # type: Dict[NewTopic, Future]

			added_topic = None  # type: NewTopic

			def get_result_method() -> str:
				nonlocal future_per_topic
				nonlocal added_topic
				nonlocal admin_client

				if added_topic is None:
					topic = next(iter(future_per_topic))
					future = future_per_topic[topic]

					# waits for the topic to be added
					future.result()

					added_topic = topic

				return added_topic

			async_handle = AsyncHandle(
				get_result_method=get_result_method
			)

			return async_handle

	def remove_topic(self, *, topic_name: str, cluster_propagation_blocking_timeout_seconds: float = 0):

		if topic_name.startswith("__"):
			raise RemoveTopicException("topic_name cannot start with \"__\".")
		else:

			admin_client = self.__get_admin_client()

			future_per_topic = admin_client.delete_topics([topic_name], operation_timeout=cluster_propagation_blocking_timeout_seconds)  # type: Dict[NewTopic, Future]

			removed_topic = None  # type: NewTopic

			def get_result_method() -> str:
				nonlocal future_per_topic
				nonlocal removed_topic
				nonlocal admin_client

				if removed_topic is None:
					topic = next(iter(future_per_topic))
					future = future_per_topic[topic]

					# waits for the topic to be removed
					future.result()

					removed_topic = topic

				return removed_topic

			async_handle = AsyncHandle(
				get_result_method=get_result_method
			)

			return async_handle

	def get_topics(self) -> Tuple[str]:

		admin_client = self.__get_admin_client()

		topics_per_topic_name = admin_client.list_topics().topics

		topics = tuple([topic_name for topic_name in topics_per_topic_name.keys() if not topic_name.startswith("__")])

		return topics

	def get_async_writer(self) -> KafkaAsyncWriter:

		producer = self.__get_producer(
			is_transactional=False
		)
		return KafkaAsyncWriter(
			producer=producer
		)

	def get_transactional_writer(self) -> KafkaTransactionalWriter:

		producer = self.__get_producer(
			is_transactional=True
		)
		return KafkaTransactionalWriter(
			producer=producer
		)

	def get_reader(self, *, topic_name: str, group_name: str, is_from_beginning: bool) -> KafkaReader:

		consumer = self.__get_consumer(
			topic_name=topic_name,
			group_name=group_name,
			is_from_beginning=is_from_beginning
		)
		return KafkaReader(
			consumer=consumer,
			read_polling_seconds=self.__read_polling_seconds
		)
