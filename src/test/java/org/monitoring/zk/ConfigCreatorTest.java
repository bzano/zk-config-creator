package org.monitoring.zk;


import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.junit.ClassRule;
import org.junit.Test;

import com.salesforce.kafka.test.junit4.SharedZookeeperTestResource;

public class ConfigCreatorTest {
	private static final int TIME_OUT = 10 * 1000;
	@ClassRule
	public static final SharedZookeeperTestResource ZOOKEEPER = new SharedZookeeperTestResource();
	
	private ConfigCreator creator = new ConfigCreator(ZOOKEEPER.getZookeeperConnectString());
	
	@Test
	public void uploadProperties_should_create_nodes_path_in_zk_and_store_properties_with_version_one() {
		// GIVEN
		String path = "/x/y/z";
		// AND
		Properties properties = new Properties();
		properties.setProperty("a", "x");
		properties.setProperty("b", "y");
		properties.setProperty("c", "z");
		// WHEN
		Optional<Integer> version = creator.uploadProperties(path, properties);
		// THEN
		assertThat(version).isNotEmpty().get().isEqualTo(1);
	}
	
	@Test
	public void uploadProperties_should_store_properties_in_zk() {
		// GIVEN
		String path = "/x/y/z";
		// AND
		Properties properties = new Properties();
		properties.setProperty("a", "x");
		properties.setProperty("b", "y");
		properties.setProperty("c", "z");
		// WHEN
		creator.uploadProperties(path, properties);
		// THEN
		assertThat(readProperties(path)).isEqualTo(properties);
	}
	
	private Properties readProperties(String path) {
		final CountDownLatch connectionLatch = new CountDownLatch(1);
		try {
			ZooKeeper zk = new ZooKeeper(ZOOKEEPER.getZookeeperConnectString(), TIME_OUT, event -> {
				if(event.getState() == KeeperState.SyncConnected) {
					connectionLatch.countDown();
				}
			});
			boolean zkInitiated = connectionLatch.await(TIME_OUT, TimeUnit.MILLISECONDS);
			if(!zkInitiated) {
				throw new IOException("Zk Init failed");
			}
			Stat lastStat = zk.exists(path, false);
			byte[] data = zk.getData(path, false, lastStat);
			Properties properties = new Properties();
			properties.load(new ByteArrayInputStream(data));
			return properties;
		} catch (IOException | InterruptedException | KeeperException e) {
			System.err.println(e.getMessage());
			return null;
		}
	}
}
