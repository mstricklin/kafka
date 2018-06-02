package com.ticongeo.fame.kafka;

import lombok.extern.slf4j.Slf4j;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;

/**
 * @author mstricklin
 * @version 1.0
 */
@Slf4j
public class ZooKeeperLocal {

	ZooKeeperServerMain zooKeeperServer;

	public ZooKeeperLocal(Properties zkProperties) throws FileNotFoundException, IOException {
		QuorumPeerConfig quorumConfiguration = new QuorumPeerConfig();
		try {
			quorumConfiguration.parseProperties(zkProperties);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		zooKeeperServer = new ZooKeeperServerMain();
		final ServerConfig configuration = new ServerConfig();
		configuration.readFrom(quorumConfiguration);


		new Thread() {
			public void run() {
				try {
					log.info("zookeeper firing up...");
					zooKeeperServer.runFromConfig(configuration);
					log.info("zookeeper started");
				} catch (IOException e) {
					log.error("ZooKeeper Failed");
					e.printStackTrace(System.err);
				}
			}
		}.start();
	}
}