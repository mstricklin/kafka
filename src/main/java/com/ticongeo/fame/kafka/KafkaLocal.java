package com.ticongeo.fame.kafka;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.Properties;

/**
 * @author mstricklin
 * @version 1.0
 */
@Slf4j
public class KafkaLocal {

	@SuppressWarnings("unused")
	private static final Logger CLASS_LOGGER = LoggerFactory.getLogger((new Throwable()).getStackTrace()[0].getClassName());

	@SuppressWarnings("unused")
	private static final String NEWLINE = System.getProperty("line.separator");
	public KafkaServerStartable kafka;
	public ZooKeeperLocal zookeeper;

	public KafkaLocal(Properties kafkaProperties, Properties zkProperties) throws IOException, InterruptedException{
		KafkaConfig kafkaConfig = new KafkaConfig(kafkaProperties);

		//start local zookeeper
		log.info("starting local zookeeper...");
		zookeeper = new ZooKeeperLocal(zkProperties);
		System.out.println("done");

		//start local kafka broker
		kafka = new KafkaServerStartable(kafkaConfig);
		log.info("starting local kafka broker...");
		kafka.startup();
		log.info("KafkaLocal done");
	}


	public void stop(){
		//stop kafka broker
		log.info("stopping kafka...");
		kafka.shutdown();
		log.info("done");
	}
}