from __future__ import annotations
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import Producer, Consumer, Message, KafkaError, TopicPartition, OFFSET_BEGINNING, OFFSET_END
from concurrent.futures import Future
from typing import List, Tuple, Dict, Callable
from austin_heller_repo.threading import Semaphore, TimeoutThread, BooleanReference, start_thread, AsyncHandle, ReadOnlyAsyncHandle
from austin_heller_repo.common import HostPointer
import uuid
import random
import time
import itertools
from datetime import datetime


class AddTopicException(Exception):

	def __init__(self, *args):
		super().__init__(*args)


class RemoveTopicException(Exception):

	def __init__(self, *args):
		super().__init__(*args)


class TopicNotFoundException(Exception):

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


class ReplicatedBrokersTotalMismatchException(Exception):

	def __init__(self, *args):
		super().__init__(*args)


class KafkaWrapper():

	def __init__(self, *, host_pointer: HostPointer):

		self.__host_pointer = host_pointer

		self.__admin_client = None  # type: AdminClient
		self.__admin_client_semaphore = Semaphore()
		self.__async_producer = None  # type: Producer
		self.__async_producer_semaphore = Semaphore()
		self.__consumers = []  # type: List[Consumer]
		self.__consumers_semaphore = Semaphore()
		self.__write_transaction = None

	def __get_bootstrap_servers(self) -> str:
		return f"{self.__host_pointer.get_host_address()}:{self.__host_pointer.get_host_port()}"

	def get_admin_client(self) -> AdminClient:

		self.__admin_client_semaphore.acquire()
		if self.__admin_client is None:
			self.__admin_client = AdminClient({
				"bootstrap.servers": self.__get_bootstrap_servers()
			})
		self.__admin_client_semaphore.release()
		return self.__admin_client

	def get_producer(self, *, is_transactional: bool) -> Producer:

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
					"bootstrap.servers": self.__get_bootstrap_servers(),
					"batch.num.messages": 1
					#"batch.size": 1
					#"linger.ms": 1
				})
			self.__async_producer_semaphore.release()
			producer = self.__async_producer
		return producer

	def get_consumer(self, *, topic_name: str, is_from_beginning: bool):

		if topic_name.startswith("^"):
			raise ReadMessageException(f"topic_name cannot start with \"^\" character.")
		self.__consumers_semaphore.acquire()
		consumer = Consumer({
			"bootstrap.servers": self.__get_bootstrap_servers(),
			"group.id": str(uuid.uuid4()),
			"auto.offset.reset": "earliest" if is_from_beginning else "latest",
			#"queued.min.messages": 1
		})

		admin_client = self.get_admin_client()

		topic_list = admin_client.list_topics().topics

		if is_from_beginning:
			partition_seek_index = OFFSET_BEGINNING
		else:
			partition_seek_index = OFFSET_END

		topic_partitions = []  # type: List[TopicPartition]
		for partition_index, _ in enumerate(topic_list[topic_name].partitions):
			topic_partition = TopicPartition(
				topic_name,
				partition_index,
				partition_seek_index
			)
			topic_partitions.append(topic_partition)

		consumer.assign(topic_partitions)

		self.__consumers.append(consumer)
		self.__consumers_semaphore.release()
		return consumer


class KafkaTopicSeekIndex():

	def __init__(self, *, topic_name: str, partition_indexes: Tuple[int]):

		self.__topic_name = topic_name
		self.__partition_indexes = partition_indexes

	def get_topic_name(self) -> str:
		return self.__topic_name

	def get_partition_indexes(self) -> Tuple[int]:
		return self.__partition_indexes

	def __eq__(self, other):
		if type(other) is type(self):
			return (self.__topic_name, self.__partition_indexes) == (other.__topic_name, other.__partition_indexes)
		else:
			return False

	def __hash__(self):
		return hash((self.__topic_name, self.__partition_indexes))


class KafkaMessage():

	def __init__(self, *, message_bytes: bytes, partition_index: int, offset: int, topic_name: str):

		self.__message_bytes = message_bytes
		self.__partition_index = partition_index
		self.__offset = offset
		self.__topic_name = topic_name

	def get_message_bytes(self) -> bytes:
		return self.__message_bytes

	def get_partition_index(self) -> int:
		return self.__partition_index

	def get_offset(self) -> int:
		return self.__offset

	def get_topic_name(self) -> str:
		return self.__topic_name


class KafkaAsyncWriter():

	def __init__(self, *, kafka_manager: KafkaManager, producer: Producer):

		self.__kafka_manager = kafka_manager
		self.__producer = producer

		self.__partitions_total = None

	def write_message(self, *, topic_name: str, message_bytes: bytes) -> AsyncHandle:

		if not isinstance(message_bytes, bytes):
			raise WriteMessageException("message_bytes object must be of type \"bytes\".")

		callback_error = None  # type: KafkaError
		callback_message = None  # type: Message
		callback_semaphore = Semaphore()
		callback_semaphore.acquire()

		def callback(error: KafkaError, message: Message):
			nonlocal callback_error
			nonlocal callback_message
			nonlocal callback_semaphore

			callback_error = error
			callback_message = message

			if callback_error is None and callback_message.error() is not None:
				callback_error = callback_message.error()

			callback_semaphore.release()

		def get_result(read_only_async_handle: ReadOnlyAsyncHandle) -> KafkaMessage:
			nonlocal callback_error
			nonlocal callback_message
			nonlocal callback_semaphore
			nonlocal topic_name

			if self.__partitions_total is None:
				kafka_partitions_async_handle = self.__kafka_manager.get_partitions(
					topic_name=topic_name
				)
				kafka_partitions_async_handle.add_parent(
					async_handle=read_only_async_handle
				)
				kafka_partitions = kafka_partitions_async_handle.get_result()  # type: List[KafkaPartition]

				if not read_only_async_handle.is_cancelled():
					self.__partitions_total = len(kafka_partitions)

			# TODO ensure that the KafkaTransactionalWriter can write each message to the same partition to ensure order
			random_partition_index = random.randrange(self.__partitions_total)

			self.__producer.produce(topic_name, message_bytes, partition=random_partition_index, callback=callback)

			self.__producer.flush()

			# poll is_cancelled while the semaphore waits

			is_waiting = True

			def wait_thread_method():
				nonlocal is_waiting
				nonlocal callback_semaphore

				callback_semaphore.acquire()
				is_waiting = False
				callback_semaphore.release()

			wait_thread = start_thread(wait_thread_method)
			while is_waiting:
				if not read_only_async_handle.is_cancelled():
					time.sleep(0.001)  # TODO restructure to not poll

			# finished waiting at this point

			if callback_error is not None:
				raise WriteMessageException(callback_error)
			else:
				kafka_message = KafkaMessage(
					message_bytes=callback_message.value(),
					partition_index=callback_message.partition(),
					offset=callback_message.offset(),
					topic_name=topic_name
				)
				return kafka_message

		async_handle = AsyncHandle(
			get_result_method=get_result
		)
		async_handle.try_wait(
			timeout_seconds=0
		)

		return async_handle


class KafkaTransactionalWriter():

	def __init__(self, *, kafka_manager: KafkaManager, producer: Producer):

		self.__kafka_manager = kafka_manager
		self.__producer = producer

		self.__kafka_async_writer = KafkaAsyncWriter(
			kafka_manager=kafka_manager,
			producer=producer
		)

	def write_message(self, *, topic_name: str, message_bytes: bytes) -> AsyncHandle:

		return self.__kafka_async_writer.write_message(
			topic_name=topic_name,
			message_bytes=message_bytes
		)

	def end_write_transaction(self):

		self.__producer.commit_transaction()


class KafkaReader():

	def __init__(self, *, consumer: Consumer, read_polling_seconds: float, topic_name: str, is_debug: bool = False):

		self.__consumer = consumer
		self.__read_polling_seconds = read_polling_seconds
		self.__topic_name = topic_name
		self.__is_debug = is_debug

		self.__topic_partitions = self.__consumer.assignment()  # type: List[TopicPartition]
		if len(self.__topic_partitions) == 0:
			raise Exception(f"Failed to find topic partitions for the KafkaReader.")

	def set_seek_index_to_front(self) -> AsyncHandle:

		def get_result(read_only_async_handle: ReadOnlyAsyncHandle):
			for topic_partition in self.__topic_partitions:
				if not read_only_async_handle.is_cancelled():
					self.__consumer.seek(partition=TopicPartition(
						topic_partition.topic,
						topic_partition.partition,
						OFFSET_BEGINNING
					))

		async_handle = AsyncHandle(
			get_result_method=get_result
		)
		async_handle.try_wait(
			timeout_seconds=0
		)

		return async_handle

	def set_seek_index_to_end(self):

		def get_result(read_only_async_handle: ReadOnlyAsyncHandle):
			for topic_partition in self.__topic_partitions:
				if not read_only_async_handle.is_cancelled():
					self.__consumer.seek(partition=TopicPartition(
						topic_partition.topic,
						topic_partition.partition,
						OFFSET_END
					))

		async_handle = AsyncHandle(
			get_result_method=get_result
		)
		async_handle.try_wait(
			timeout_seconds=0
		)

		return async_handle

	def set_seek_index(self, *, kafka_topic_seek_index: KafkaTopicSeekIndex):

		def get_result(read_only_async_handle: ReadOnlyAsyncHandle):
			nonlocal kafka_topic_seek_index

			for seek_index, topic_partition in zip(kafka_topic_seek_index.get_partition_indexes(), self.__topic_partitions):
				if not read_only_async_handle.is_cancelled():
					self.__consumer.seek(partition=TopicPartition(
						topic_partition.topic,
						topic_partition.partition,
						seek_index
					))

		async_handle = AsyncHandle(
			get_result_method=get_result
		)
		async_handle.try_wait(
			timeout_seconds=0
		)

		return async_handle

	def get_seek_index(self) -> AsyncHandle:

		def get_result(read_only_async_handle: ReadOnlyAsyncHandle) -> KafkaTopicSeekIndex:
			partition_indexes = []  # type: List[int]
			current_topic_partitions = self.__consumer.position(self.__topic_partitions)
			if not read_only_async_handle.is_cancelled():
				for topic_partition in current_topic_partitions:
					partition_indexes.append(topic_partition.offset)
				return KafkaTopicSeekIndex(
					topic_name=self.__topic_name,
					partition_indexes=tuple(partition_indexes)
				)

		async_handle = AsyncHandle(
			get_result_method=get_result
		)
		async_handle.try_wait(
			timeout_seconds=0
		)

		return async_handle

	def try_read_message(self, *, timeout_seconds: float) -> AsyncHandle:

		def get_result_method(read_only_async_handle: ReadOnlyAsyncHandle) -> KafkaMessage:
			nonlocal timeout_seconds

			read_async_handle = self.read_message()

			read_async_handle.add_parent(
				async_handle=read_only_async_handle
			)

			if read_async_handle.try_wait(
				timeout_seconds=timeout_seconds
			):
				return read_async_handle.get_result()
			else:
				read_async_handle.cancel()
				return None

		async_handle = AsyncHandle(
			get_result_method=get_result_method
		)

		return async_handle

	def read_message(self) -> AsyncHandle:

		def get_result_method(read_only_async_handle: ReadOnlyAsyncHandle) -> KafkaMessage:

			try:
				message = None  # type: Message

				while message is None and not read_only_async_handle.is_cancelled():
					message = self.__consumer.poll(self.__read_polling_seconds)
					if self.__is_debug:
						print(f"{datetime.utcnow()}: KafkaReader: read_message: consumer poll: {message}")
					if not read_only_async_handle.is_cancelled():
						if message is not None:
							if self.__is_debug:
								print(f"{datetime.utcnow()}: KafkaReader: read_message: found message")
							message_error = message.error()
							if message_error is not None:
								if self.__is_debug:
									print(f"{datetime.utcnow()}: KafkaReader: read_message: is error: {message_error}")
								raise ReadMessageException(message_error)
					else:
						message = None

				if message is not None:

					if self.__is_debug:
						print(f"{datetime.utcnow()}: KafkaReader: read_message: returning message")

					message_bytes = message.value()

					return KafkaMessage(
						message_bytes=message_bytes,
						partition_index=message.partition(),
						offset=message.offset(),
						topic_name=self.__topic_name
					)
				else:

					if self.__is_debug:
						print(f"{datetime.utcnow()}: KafkaReader: read_message: returning nothing")

					return None
			except Exception as ex:
				if self.__is_debug:
					print(f"{datetime.utcnow()}: KafkaReader: read_message: ex: {ex}")
				raise ex

		async_handle = AsyncHandle(
			get_result_method=get_result_method
		)
		async_handle.try_wait(
			timeout_seconds=0
		)

		return async_handle


class KafkaPartition():

	def __init__(self, *, kafka_wrapper: KafkaWrapper, kafka_partition_id: int, topic_name: str, leader_broker_id: int, replicated_broker_ids: List[int]):

		self.__kafka_wrapper = kafka_wrapper
		self.__kafka_partition_id = kafka_partition_id
		self.__topic_name = topic_name
		self.__leader_broker_id = leader_broker_id
		self.__replicated_broker_ids = replicated_broker_ids

	def get_topic_name(self) -> str:
		return self.__topic_name

	def get_leader_broker(self) -> KafkaBroker:

		admin_client = self.__kafka_wrapper.get_admin_client()

		topic_list = admin_client.list_topics()

		# TODO exception if missing broker

		kafka_broker = KafkaBroker(
			kafka_wrapper=self.__kafka_wrapper,
			kafka_broker_id=self.__leader_broker_id,
			host_pointer=HostPointer(
				host_address=topic_list.brokers[self.__leader_broker_id].host,
				host_port=topic_list.brokers[self.__leader_broker_id].port
			)
		)
		return kafka_broker

	def get_replicated_brokers(self) -> List[KafkaBroker]:

		admin_client = self.__kafka_wrapper.get_admin_client()

		topic_list = admin_client.list_topics()

		kafka_brokers = []  # type: List[KafkaBroker]
		for kafka_broker_id in topic_list.brokers:
			if kafka_broker_id in self.__replicated_broker_ids:
				kafka_broker = KafkaBroker(
					kafka_wrapper=self.__kafka_wrapper,
					kafka_broker_id=kafka_broker_id,
					host_pointer=HostPointer(
						host_address=topic_list.brokers[kafka_broker_id].host,
						host_port=topic_list.brokers[kafka_broker_id].port
					)
				)
				kafka_brokers.append(kafka_broker)
		if len(self.__replicated_broker_ids) != len(kafka_brokers):
			raise ReplicatedBrokersTotalMismatchException(f"Expected {len(self.__replicated_broker_ids)} based on partition details but found {len(kafka_brokers)}.")
		else:
			return kafka_brokers

	def is_exist(self) -> bool:
		# TODO return if broker still exists
		raise NotImplementedError()

	def refresh(self):
		# TODO throw exception if not exists
		# TODO refresh leader and replicated brokers
		raise NotImplementedError()


class KafkaBroker():

	def __init__(self, *, kafka_wrapper: KafkaWrapper, kafka_broker_id: int, host_pointer: HostPointer):

		self.__kafka_wrapper = kafka_wrapper
		self.__kafka_broker_id = kafka_broker_id
		self.__host_pointer = host_pointer

	def get_host_pointer(self) -> HostPointer:
		return self.__host_pointer

	def get_partition_per_topic(self) -> Dict[str, KafkaPartition]:

		admin_client = self.__kafka_wrapper.get_admin_client()

		topic_list = admin_client.list_topics()

		kafka_partition_per_topic = {}  # type: Dict[str, KafkaPartition]
		for topic_name in topic_list.topics:
			for kafka_partition_id in topic_list[topic_name].partitions:
				if self.__kafka_broker_id in topic_list[topic_name].partitions[kafka_partition_id].replicas:
					kafka_partition_per_topic[topic_name] = KafkaPartition(
						kafka_wrapper=self.__kafka_wrapper,
						kafka_partition_id=kafka_partition_id,
						topic_name=topic_name,
						leader_broker_id=topic_list[topic_name].partitions[kafka_partition_id].leader,
						replicated_broker_ids=topic_list[topic_name].partitions[kafka_partition_id].replicas
					)
		return kafka_partition_per_topic

	def is_exist(self) -> bool:
		# TODO return if broker still exists
		raise NotImplementedError()


class KafkaManager():

	def __init__(self, *, kafka_wrapper: KafkaWrapper, read_polling_seconds: float, is_cancelled_polling_seconds: float, new_topic_partitions_total: int, new_topic_replication_factor: int, remove_topic_cluster_propagation_blocking_timeout_seconds: int, is_debug: bool = False):

		self.__kafka_wrapper = kafka_wrapper
		self.__read_polling_seconds = read_polling_seconds
		self.__is_cancelled_polling_seconds = is_cancelled_polling_seconds
		self.__new_topic_partitions_total = new_topic_partitions_total
		self.__new_topic_replication_factor = new_topic_replication_factor
		self.__remove_topic_cluster_propagation_blocking_timeout_seconds = remove_topic_cluster_propagation_blocking_timeout_seconds
		self.__is_debug = is_debug

	def add_topic(self, *, topic_name: str) -> AsyncHandle:

		if topic_name.startswith("__"):
			raise AddTopicException("topic_name cannot start with \"__\".")
		else:

			admin_client = self.__kafka_wrapper.get_admin_client()

			topic = NewTopic(topic_name, self.__new_topic_partitions_total, self.__new_topic_replication_factor)

			future_per_topic = admin_client.create_topics([topic])  # type: Dict[NewTopic, Future]

			added_topic = None  # type: NewTopic

			def get_result(read_only_async_handle: ReadOnlyAsyncHandle) -> str:
				nonlocal future_per_topic
				nonlocal added_topic
				nonlocal admin_client

				if added_topic is None:
					topic = next(iter(future_per_topic))
					future = future_per_topic[topic]

					# waits for the topic to be added
					future.result()

					added_topic = topic

					while not read_only_async_handle.is_cancelled():
						all_topics_async_handle = self.get_topics()
						all_topics_async_handle.add_parent(
							async_handle=read_only_async_handle
						)
						all_topics = all_topics_async_handle.get_result()  # type: List[str]

						if added_topic not in all_topics:
							time.sleep(self.__is_cancelled_polling_seconds)
						else:
							break

				return added_topic

			async_handle = AsyncHandle(
				get_result_method=get_result
			)
			async_handle.try_wait(
				timeout_seconds=0
			)

			return async_handle

	def remove_topic(self, *, topic_name: str) -> AsyncHandle:

		if topic_name.startswith("__"):
			raise RemoveTopicException("topic_name cannot start with \"__\".")
		else:

			admin_client = self.__kafka_wrapper.get_admin_client()

			future_per_topic = admin_client.delete_topics([topic_name])  # type: Dict[NewTopic, Future]

			removed_topic = None  # type: NewTopic

			def get_result(read_only_async_handle: ReadOnlyAsyncHandle) -> str:
				nonlocal future_per_topic
				nonlocal removed_topic
				nonlocal admin_client

				if removed_topic is None:
					topic = next(iter(future_per_topic))
					future = future_per_topic[topic]

					# waits for the topic to be removed
					future.result()

					removed_topic = topic

					while not read_only_async_handle.is_cancelled():
						all_topics_async_handle = self.get_topics()
						all_topics_async_handle.add_parent(
							async_handle=read_only_async_handle
						)
						all_topics = all_topics_async_handle.get_result()  # type: List[str]

						if removed_topic in all_topics:
							time.sleep(self.__is_cancelled_polling_seconds)
						else:
							break

				return removed_topic

			async_handle = AsyncHandle(
				get_result_method=get_result
			)
			async_handle.try_wait(
				timeout_seconds=0
			)

			return async_handle

	def get_topics(self) -> AsyncHandle:

		def get_result(read_only_async_handle: ReadOnlyAsyncHandle):
			topics = None  # type: Tuple[str]
			if not read_only_async_handle.is_cancelled():
				admin_client = self.__kafka_wrapper.get_admin_client()
				if not read_only_async_handle.is_cancelled():
					topic_list = admin_client.list_topics()
					if not read_only_async_handle.is_cancelled():
						topics_per_topic_name = topic_list.topics
						if not read_only_async_handle.is_cancelled():
							topics = tuple([topic_name for topic_name in topics_per_topic_name.keys() if not topic_name.startswith("__")])
			return topics

		async_handle = AsyncHandle(
			get_result_method=get_result
		)
		async_handle.try_wait(
			timeout_seconds=0
		)

		return async_handle

	def get_partitions(self, *, topic_name: str) -> AsyncHandle:

		def get_result(read_only_async_handle: ReadOnlyAsyncHandle):
			kafka_partitions = None  # type: List[KafkaPartition]
			if not read_only_async_handle.is_cancelled():
				admin_client = self.__kafka_wrapper.get_admin_client()
				if not read_only_async_handle.is_cancelled():
					topic_list = admin_client.list_topics()
					if not read_only_async_handle.is_cancelled():
						if topic_name not in topic_list.topics.keys():
							raise TopicNotFoundException(f"Topic name: \"{topic_name}\".")
						else:
							kafka_partitions = []
							for partition_kafka_id in topic_list.topics[topic_name].partitions:
								kafka_partition = KafkaPartition(
									kafka_wrapper=self.__kafka_wrapper,
									kafka_partition_id=partition_kafka_id,
									topic_name=topic_name,
									leader_broker_id=topic_list.topics[topic_name].partitions[partition_kafka_id].leader,
									replicated_broker_ids=topic_list.topics[topic_name].partitions[partition_kafka_id].replicas
								)
								kafka_partitions.append(kafka_partition)
			return kafka_partitions

		async_handle = AsyncHandle(
			get_result_method=get_result
		)
		async_handle.try_wait(
			timeout_seconds=0
		)

		return async_handle

	def get_brokers(self) -> AsyncHandle:

		def get_result(read_only_async_handle: ReadOnlyAsyncHandle):
			kafka_brokers = None  # type: List[KafkaBroker]
			if not read_only_async_handle.is_cancelled():
				admin_client = self.__kafka_wrapper.get_admin_client()
				if not read_only_async_handle.is_cancelled():
					topic_list = admin_client.list_topics()
					if not read_only_async_handle.is_cancelled():
						kafka_brokers = []
						for kafka_broker_id in topic_list.brokers:
							kafka_broker = KafkaBroker(
								kafka_wrapper=self.__kafka_wrapper,
								kafka_broker_id=kafka_broker_id,
								host_pointer=HostPointer(
									host_address=topic_list.brokers[kafka_broker_id].host,
									host_port=topic_list.brokers[kafka_broker_id].port
								)
							)
							kafka_brokers.append(kafka_broker)
			return kafka_brokers

		async_handle = AsyncHandle(
			get_result_method=get_result
		)
		async_handle.try_wait(
			timeout_seconds=0
		)

		return async_handle

	def get_async_writer(self) -> AsyncHandle:

		def get_result(read_only_async_handle: ReadOnlyAsyncHandle):
			kafka_writer = None
			if not read_only_async_handle.is_cancelled():
				producer = self.__kafka_wrapper.get_producer(
					is_transactional=False
				)
				if not read_only_async_handle.is_cancelled():
					kafka_writer = KafkaAsyncWriter(
						kafka_manager=self,
						producer=producer
					)
			return kafka_writer

		async_handle = AsyncHandle(
			get_result_method=get_result
		)
		async_handle.try_wait(
			timeout_seconds=0
		)
		return async_handle

	def get_transactional_writer(self) -> AsyncHandle:

		def get_result(read_only_async_handle: ReadOnlyAsyncHandle):
			kafka_writer = None
			if not read_only_async_handle.is_cancelled():
				producer = self.__kafka_wrapper.get_producer(
					is_transactional=True
				)
				if not read_only_async_handle.is_cancelled():
					kafka_writer = KafkaTransactionalWriter(
						kafka_manager=self,
						producer=producer
					)
			return kafka_writer

		async_handle = AsyncHandle(
			get_result_method=get_result
		)
		async_handle.try_wait(
			timeout_seconds=0
		)
		return async_handle

	def get_reader(self, *, topic_name: str, is_from_beginning: bool) -> AsyncHandle:

		def get_result(read_only_async_handle) -> KafkaReader:
			consumer = self.__kafka_wrapper.get_consumer(
				topic_name=topic_name,
				is_from_beginning=is_from_beginning
			)

			while len(consumer.assignment()) == 0 and not read_only_async_handle.is_cancelled():
				time.sleep(0.01)

			if read_only_async_handle.is_cancelled():
				kafka_reader = None
			else:
				kafka_reader = KafkaReader(
					consumer=consumer,
					read_polling_seconds=self.__read_polling_seconds,
					topic_name=topic_name,
					is_debug=self.__is_debug
				)

			return kafka_reader

		async_handle = AsyncHandle(
			get_result_method=get_result
		)
		async_handle.try_wait(
			timeout_seconds=0
		)

		return async_handle

	def get_messages(self, *, topic_name: str, end_of_topic_read_timeout_seconds: float) -> AsyncHandle:

		def get_result(read_only_async_handle: ReadOnlyAsyncHandle):
			if not read_only_async_handle.is_cancelled():
				messages = []  # type: List[KafkaMessage]

				kafka_reader_async_handle = self.get_reader(
					topic_name=topic_name,
					is_from_beginning=True
				)
				kafka_reader_async_handle.add_parent(
					async_handle=read_only_async_handle
				)
				kafka_reader = kafka_reader_async_handle.get_result()  # type: KafkaReader

				if not read_only_async_handle.is_cancelled():
					is_last_message_found = False
					while not is_last_message_found:
						message_async_handle = kafka_reader.try_read_message(
							timeout_seconds=end_of_topic_read_timeout_seconds
						)
						message_async_handle.add_parent(
							async_handle=read_only_async_handle
						)
						message = message_async_handle.get_result()  # type: KafkaMessage

						if not read_only_async_handle.is_cancelled():
							if message is None:
								is_last_message_found = True
							else:
								messages.append(message)
				return messages

		async_handle = AsyncHandle(
			get_result_method=get_result
		)
		async_handle.try_wait(
			timeout_seconds=0
		)

		return async_handle

	def get_kafka_topic_seek_index_from_kafka_message(self, kafka_message: KafkaMessage) -> AsyncHandle:

		def get_result(read_only_async_handle: ReadOnlyAsyncHandle):
			kafka_topic_seek_index = None  # type: KafkaTopicSeekIndex
			if not read_only_async_handle.is_cancelled():
				partition_indexes = []  # type: List[int]
				partitions_async_handle = self.get_partitions(
					topic_name=kafka_message.get_topic_name()
				)
				partitions_async_handle.add_parent(
					async_handle=read_only_async_handle
				)
				partitions = partitions_async_handle.get_result()  # type: List[KafkaPartition]

				if not read_only_async_handle.is_cancelled():
					for topic_partition_index, topic_partition in enumerate(partitions):
						if topic_partition_index == kafka_message.get_partition_index():
							seek_index = kafka_message.get_offset()
						else:
							seek_index = OFFSET_END
						partition_indexes.append(seek_index)
					kafka_topic_seek_index = KafkaTopicSeekIndex(
						topic_name=kafka_message.get_topic_name(),
						partition_indexes=tuple(partition_indexes)
					)
			return kafka_topic_seek_index

		async_handle = AsyncHandle(
			get_result_method=get_result
		)
		async_handle.try_wait(
			timeout_seconds=0
		)

		return async_handle


class KafkaManagerFactory():

	def __init__(self, *, kafka_wrapper: KafkaWrapper, read_polling_seconds: float, is_cancelled_polling_seconds: float, new_topic_partitions_total: int, new_topic_replication_factor: int, remove_topic_cluster_propagation_blocking_timeout_seconds: int, is_debug: bool = False):

		self.__kafka_wrapper = kafka_wrapper
		self.__read_polling_seconds = read_polling_seconds
		self.__is_cancelled_polling_seconds = is_cancelled_polling_seconds
		self.__new_topic_partitions_total = new_topic_partitions_total
		self.__new_topic_replication_factor = new_topic_replication_factor
		self.__remove_topic_cluster_propagation_blocking_timeout_seconds = remove_topic_cluster_propagation_blocking_timeout_seconds
		self.__is_debug = is_debug

	def get_kafka_manager(self) -> KafkaManager:

		return KafkaManager(
			kafka_wrapper=self.__kafka_wrapper,
			read_polling_seconds=self.__read_polling_seconds,
			is_cancelled_polling_seconds=self.__is_cancelled_polling_seconds,
			new_topic_partitions_total=self.__new_topic_partitions_total,
			new_topic_replication_factor=self.__new_topic_replication_factor,
			remove_topic_cluster_propagation_blocking_timeout_seconds=self.__remove_topic_cluster_propagation_blocking_timeout_seconds,
			is_debug=self.__is_debug
		)
