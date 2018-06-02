package com.ticongeo.fame.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import static com.ticongeo.fame.kafka.NodeE.TOPIC;

@Slf4j
public class Application {

	public static void main(String[] args) throws Exception {
		log.info("Shazam");

		Properties kafkaProperties = getKafkaProperties(DEFAULT_KAFKA_LOG_DIR, BROKER_PORT, BROKER_ID);
		Properties zkProperties = getZookeeperProperties(ZOOKEEPER_PORT,DEFAULT_ZOOKEEPER_LOG_DIR);

		//start kafka
		KafkaLocal kafka = new KafkaLocal(kafkaProperties, zkProperties);
		Thread.sleep(10000);
		log.info("starting run.....");

		KafkaProducer<String, String> nodeE = NodeE.of();
		((NodeE) nodeE).run();

		Topology builder = new Topology();
		builder.addSource("SOURCE", new StringDeserializer(), new StringDeserializer(), TOPIC);
		builder.addProcessor("PROCESS", NodeK::new, "SOURCE");
		Thread.sleep(50000);
	}


	private static Properties getKafkaProperties(String logDir, int port, int brokerId) {
		Properties properties = new Properties();
		properties.put("port", port + "");
		properties.put("broker.id", brokerId + "");
		properties.put("log.dir", logDir);
		properties.put("zookeeper.connect", ZOOKEEPER_HOST);
		properties.put("default.replication.factor", "1");
		properties.put("delete.topic.enable", "true");
		return properties;
	}

	private static Properties getZookeeperProperties(int port, String zookeeperDir) {
		Properties properties = new Properties();
		properties.put("clientPort", port + "");
		properties.put("dataDir", zookeeperDir);
		return properties;
	}


	private static final String DEFAULT_KAFKA_LOG_DIR = "/tmp/test/kafka_embedded";
	private static final String TEST_TOPIC = "test_topic";
	private static final int BROKER_ID = 1;
	private static final int BROKER_PORT = 50000;
	private static final String LOCALHOST_BROKER = String.format("localhost:%d", BROKER_PORT);

	private static final String DEFAULT_ZOOKEEPER_LOG_DIR = "/tmp/test/zookeeper";
	private static final int ZOOKEEPER_PORT = 20000;
	private static final String ZOOKEEPER_HOST = String.format("localhost:%d", ZOOKEEPER_PORT);

}


