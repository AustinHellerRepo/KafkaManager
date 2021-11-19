from __future__ import annotations
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import Producer, Consumer, Message, KafkaError, TopicPartition
from concurrent.futures import Future
from typing import List, Tuple, Dict, Callable
from austin_heller_repo.threading import Semaphore, TimeoutThread
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

	def __init__(self, *, host_url: str, host_port: int):

		self.__host_url = host_url
		self.__host_port = host_port

		self.__admin_client = None  # type: AdminClient
		self.__admin_client_semaphore = Semaphore()
		self.__async_producer = None  # type: Producer
		self.__async_producer_semaphore = Semaphore()
		self.__consumers = []  # type: List[Consumer]
		self.__consumers_semaphore = Semaphore()
		self.__write_transaction = None

	def __get_bootstrap_servers(self) -> str:
		return f"{self.__host_url}:{self.__host_port}"

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
					"bootstrap.servers": self.__get_bootstrap_servers()
				})
			self.__async_producer_semaphore.release()
			producer = self.__async_producer
		return producer

	def get_consumer(self, *, topic_name: str, group_name: str, is_from_beginning: bool):

		if topic_name.startswith("^"):
			raise ReadMessageException(f"topic_name cannot start with \"^\" character.")
		self.__consumers_semaphore.acquire()
		consumer = Consumer({
			"bootstrap.servers": self.__get_bootstrap_servers(),
			"group.id": group_name,
			"auto.offset.reset": "earliest" if is_from_beginning else "latest",
			"queued.min.messages": 1
		})

		consumer.subscribe([topic_name])
		self.__consumers.append(consumer)
		self.__consumers_semaphore.release()
		return consumer


class AsyncHandle():

	def __init__(self, *, get_result_method: Callable[[], object]):

		self.__get_result_method = get_result_method

		self.__is_storing = None
		self.__result = None
		self.__wait_for_result_semaphore = Semaphore()

	def __store_result(self):

		self.__wait_for_result_semaphore.acquire()
		self.__is_storing = True
		self.__result = self.__get_result_method()
		self.__is_storing = False
		self.__wait_for_result_semaphore.release()

	def __wait_for_result(self):

		self.__wait_for_result_semaphore.acquire()
		self.__wait_for_result_semaphore.release()

	def try_wait(self, *, timeout_seconds: float) -> bool:

		if self.__is_storing is None:
			timeout_thread = TimeoutThread(self.__store_result, timeout_seconds)
			timeout_thread.start()
			is_successful = timeout_thread.try_wait()
		elif self.__is_storing:
			timeout_thread = TimeoutThread(self.__wait_for_result, timeout_seconds)
			timeout_thread.start()
			is_successful = timeout_thread.try_wait()
		else:
			is_successful = True

		return is_successful

	def get_result(self) -> object:

		if self.__is_storing is None:
			self.__store_result()
		elif self.__is_storing:
			self.__wait_for_result_semaphore.acquire()
			self.__wait_for_result_semaphore.release()

		return self.__result


class KafkaAsyncWriter():

	def __init__(self, *, kafka_manager: KafkaManager, producer: Producer):

		self.__kafka_manager = kafka_manager
		self.__producer = producer

	def write_message(self, *, topic_name: str, message_bytes: bytes) -> AsyncHandle:

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

		kafka_partitions = self.__kafka_manager.get_partitions(
			topic_name=topic_name
		)

		random_partition_index = random.randrange(len(kafka_partitions))

		self.__producer.produce(topic_name, message_bytes, partition=random_partition_index, callback=callback)

		async_handle = AsyncHandle(
			get_result_method=get_result_method
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
			host_url=topic_list.brokers[self.__leader_broker_id].host,
			host_port=topic_list.brokers[self.__leader_broker_id].port
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
					host_url=topic_list.brokers[kafka_broker_id].host,
					host_port=topic_list.brokers[kafka_broker_id].port
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

	def __init__(self, *, kafka_wrapper: KafkaWrapper, kafka_broker_id: int, host_url: str, host_port: int):

		self.__kafka_wrapper = kafka_wrapper
		self.__kafka_broker_id = kafka_broker_id
		self.__host_url = host_url
		self.__host_port = host_port

	def get_host_url(self) -> str:
		return self.__host_url

	def get_host_port(self) -> int:
		return self.__host_port

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


# TODO each read will need to be checked against a db to ensure that this project has already read this topic's partition's message offset
class KafkaManager():

	def __init__(self, *, kafka_wrapper: KafkaWrapper, read_polling_seconds: float, cluster_propagation_seconds: float, new_topic_partitions_total: int, new_topic_replication_factor: int, remove_topic_cluster_propagation_blocking_timeout_seconds: int):

		self.__kafka_wrapper = kafka_wrapper
		self.__read_polling_seconds = read_polling_seconds
		self.__cluster_propagation_seconds = cluster_propagation_seconds
		self.__new_topic_partitions_total = new_topic_partitions_total
		self.__new_topic_replication_factor = new_topic_replication_factor
		self.__remove_topic_cluster_propagation_blocking_timeout_seconds = remove_topic_cluster_propagation_blocking_timeout_seconds

	def add_topic(self, *, topic_name: str) -> AsyncHandle:

		if topic_name.startswith("__"):
			raise AddTopicException("topic_name cannot start with \"__\".")
		else:

			admin_client = self.__kafka_wrapper.get_admin_client()

			topic = NewTopic(topic_name, self.__new_topic_partitions_total, self.__new_topic_replication_factor)

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

					verification_delay_seconds = 0.01
					maximum_verification_iterations_total = int(self.__cluster_propagation_seconds / verification_delay_seconds)
					is_verified = False
					for verification_index in range(maximum_verification_iterations_total):
						all_topics = self.get_topics()
						if added_topic not in all_topics:
							time.sleep(verification_delay_seconds)
						else:
							is_verified = True
							break
					if not is_verified:
						raise AddTopicException(f"Failed to verify topic exists after {self.__cluster_propagation_seconds} seconds.")

				return added_topic

			async_handle = AsyncHandle(
				get_result_method=get_result_method
			)

			return async_handle

	def remove_topic(self, *, topic_name: str) -> AsyncHandle:

		if topic_name.startswith("__"):
			raise RemoveTopicException("topic_name cannot start with \"__\".")
		else:

			admin_client = self.__kafka_wrapper.get_admin_client()

			future_per_topic = admin_client.delete_topics([topic_name])  # type: Dict[NewTopic, Future]

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

					verification_delay_seconds = 0.01
					maximum_verification_iterations_total = int(self.__cluster_propagation_seconds / verification_delay_seconds)
					is_verified = False
					for verification_index in range(maximum_verification_iterations_total):
						all_topics = self.get_topics()
						if removed_topic in all_topics:
							time.sleep(verification_delay_seconds)
						else:
							is_verified = True
							break
					if not is_verified:
						raise RemoveTopicException(f"Failed to verify topic no longer exists after {self.__cluster_propagation_seconds} seconds.")

				return removed_topic

			async_handle = AsyncHandle(
				get_result_method=get_result_method
			)

			return async_handle

	def get_topics(self) -> Tuple[str]:

		admin_client = self.__kafka_wrapper.get_admin_client()

		topic_list = admin_client.list_topics()

		topics_per_topic_name = topic_list.topics

		topics = tuple([topic_name for topic_name in topics_per_topic_name.keys() if not topic_name.startswith("__")])

		return topics

	def get_partitions(self, *, topic_name: str) -> List[KafkaPartition]:

		admin_client = self.__kafka_wrapper.get_admin_client()

		topic_list = admin_client.list_topics()

		if topic_name not in topic_list.topics.keys():
			raise TopicNotFoundException(f"Topic name: \"{topic_name}\".")

		kafka_partitions = []  # type: List[KafkaPartition]
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

	def get_brokers(self) -> List[KafkaBroker]:

		admin_client = self.__kafka_wrapper.get_admin_client()

		topic_list = admin_client.list_topics()

		kafka_brokers = []  # type: List[KafkaBroker]
		for kafka_broker_id in topic_list.brokers:
			kafka_broker = KafkaBroker(
				kafka_wrapper=self.__kafka_wrapper,
				kafka_broker_id=kafka_broker_id,
				host_url=topic_list.brokers[kafka_broker_id].host,
				host_port=topic_list.brokers[kafka_broker_id].port
			)
			kafka_brokers.append(kafka_broker)
		return kafka_brokers

	def get_async_writer(self) -> KafkaAsyncWriter:

		producer = self.__kafka_wrapper.get_producer(
			is_transactional=False
		)
		return KafkaAsyncWriter(
			kafka_manager=self,
			producer=producer
		)

	def get_transactional_writer(self) -> KafkaTransactionalWriter:

		producer = self.__kafka_wrapper.get_producer(
			is_transactional=True
		)
		return KafkaTransactionalWriter(
			kafka_manager=self,
			producer=producer
		)

	def get_reader(self, *, topic_name: str, group_name: str, is_from_beginning: bool) -> KafkaReader:

		consumer = self.__kafka_wrapper.get_consumer(
			topic_name=topic_name,
			group_name=group_name,
			is_from_beginning=is_from_beginning
		)

		return KafkaReader(
			consumer=consumer,
			read_polling_seconds=self.__read_polling_seconds
		)
