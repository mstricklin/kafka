package com.ticongeo.fame.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.time.Instant;
import java.util.Map;
import java.util.Properties;

/**
 * @author mstricklin
 * @version 1.0
 */
// TODO: make a true producer node, and hand it off to a KafkaProducer wrappery thing
public class NodeE extends KafkaProducer<String, String> implements Runnable {

	public static String TOPIC = "timer-tip-events";
	@SuppressWarnings("unused")
	private static final Logger CLASS_LOGGER = LoggerFactory.getLogger((new Throwable()).getStackTrace()[0].getClassName());

	@SuppressWarnings("unused")
	private static final String NEWLINE = System.getProperty("line.separator");

	public static NodeE of() {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		return new NodeE(props);
	}

	public NodeE(Properties properties) {
		super(properties);
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		CLASS_LOGGER.warn("com.ticongeo.fame.kafka.NodeE.run is not currently implemented");
		while (true) {
			try {
				Thread.sleep(1000);
				Instant now = Instant.now();
				send(new ProducerRecord<String, String>(TOPIC,
				                                   "Event", now.toString()));
			} catch (InterruptedException e) {
				CLASS_LOGGER.error(e.toString());
			}
		}

	}
}