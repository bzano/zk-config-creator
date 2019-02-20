package org.monitoring.zk;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigCreator {
	private static final Logger sLog = LoggerFactory.getLogger(ConfigCreator.class);
	
	private static final int TIME_OUT = 10 * 1000;
	private static final String ZK_NODE_SEP = "/";
	private static final String EMPTY = "";
	private String zookeeperService;
	
	public ConfigCreator(String zookeeper) {
		this.zookeeperService = zookeeper;
	}
	
	public Optional<Integer> uploadProperties(String path, Properties properties) {
		return createZookeeperClient().flatMap(zk -> {
			List<String> nodesPaths = getNodesPaths(path);
			nodesPaths.forEach(node -> createNode(zk, node));
			return propsToBytes(properties).flatMap(data -> putDataIntoNode(zk, path, data));
		});
	}
	
	private Optional<Integer> putDataIntoNode(ZooKeeper zk, String path, byte[] data) {
		try {
			Stat oldStat = zk.exists(path, false);
			Stat stat = zk.setData(path, data, oldStat.getVersion());
			sLog.info("Operation completed ::: version of node (" + path + ") is now " + stat.getVersion());
			return Optional.ofNullable(stat.getVersion());
		} catch (KeeperException | InterruptedException e) {
			sLog.error("Error set node (" + path + ") " + e.getMessage());
			return Optional.empty();
		}
	}
	
	private void createNode(ZooKeeper zk, String node) {
		try {
			zk.create(node, EMPTY.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		} catch (NodeExistsException nee) {
			sLog.info("Node (" + node + ") already exists");
		} catch (KeeperException | InterruptedException e) {
			sLog.error("Error creating node (" + node + ") " + e.getMessage());
		}
	}
	
	private Optional<byte[]> propsToBytes(Properties properties){
		try {
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			properties.store(out, EMPTY);
			return Optional.of(out.toByteArray());
		} catch (IOException e) {
			sLog.error("Props to bytes" + e.getMessage());
			return Optional.empty();
		}
	}

	private List<String> getNodesPaths(String path) {
		List<String> nodes = Arrays.asList(path.split(ZK_NODE_SEP));
		List<String> pathNodes = nodes.stream()
			.filter(x -> x.length() > 0)
			.map(node -> ZK_NODE_SEP + node)
			.reduce(new ArrayList<String>(), this::addElementToList, this::combineLists);
		return pathNodes;
	}
	
	private List<String> addElementToList(List<String> list, String element){
		String newU = list.stream().reduce((f, s) -> s).map(last -> last + element).orElse(element);
		list.add(newU);
		return list;
	}
	
	private List<String> combineLists(List<String> first, List<String> second){
		first.addAll(second);
		return first;
	}
	
	private Optional<ZooKeeper> createZookeeperClient(){
		return Optional.ofNullable(zookeeperService)
			.map(srv -> {
				final CountDownLatch connectionLatch = new CountDownLatch(1);
				try {
					ZooKeeper zk = new ZooKeeper(srv, TIME_OUT, event -> {
						if(event.getState() == KeeperState.SyncConnected) {
							connectionLatch.countDown();
						}
					});
					boolean zkInitiated = connectionLatch.await(TIME_OUT, TimeUnit.MILLISECONDS);
					if(!zkInitiated) {
						throw new IOException("Zk (TIME OUT)");
					}
					sLog.info("Zk client conneted to (" + zookeeperService + ")");
					return zk;
				} catch (IOException | InterruptedException e) {
					sLog.error("Connection to zk failed " + e.getMessage());
					return null;
				}
			});
	}
	
}
