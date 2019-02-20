package org.monitoring.zk;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MakeConfig {
	private static final Logger sLog;
	static {
		URL resource = ClassLoader.getSystemResource("log4j-fixed.properties");
		if(resource == null) {
			resource = ClassLoader.getSystemResource("resources/log4j-fixed.properties");
		}
		String filePath = resource.getProtocol() + ":" + resource.getPath();
		System.setProperty("log4j.configuration", filePath);
		sLog = LoggerFactory.getLogger(MakeConfig.class);
	}
	
	private static final String CONFIG_PATH = "c";
	private static final String CONFIG_PATH_LONG = "config";
	private static final String CONFIG_PATH_DESCRIPTION = "Local config path";
	private static final Option CONFIG_PATH_OPT = Option.builder().required()
			.argName(CONFIG_PATH).longOpt(CONFIG_PATH_LONG)
			.desc(CONFIG_PATH_DESCRIPTION).hasArg().build();
	
	private static final String ZK_NODE_PATH = "z";
	private static final String ZK_NODE_PATH_LONG = "zkPath";
	private static final String ZK_NODE_PATH_DESCRIPTION = "Zookeeper node path";
	private static final Option ZK_NODE_PATH_OPT = Option.builder().required()
			.argName(ZK_NODE_PATH).longOpt(ZK_NODE_PATH_LONG)
			.desc(ZK_NODE_PATH_DESCRIPTION).hasArg().build();
	
	private static final String ZK_SERVERS = "s";
	private static final String ZK_SERVERS_LONG = "zkServers";
	private static final String ZK_SERVERS_DESCRIPTION = "Zookeeper servers";
	private static final Option ZK_SERVERS_OPT = Option.builder().required()
			.argName(ZK_SERVERS).longOpt(ZK_SERVERS_LONG)
			.desc(ZK_SERVERS_DESCRIPTION).hasArg().build();
	
	public static void main(String[] args) {
		Options options = new Options();
		options.addOption(ZK_SERVERS_OPT);
		options.addOption(CONFIG_PATH_OPT);
		options.addOption(ZK_NODE_PATH_OPT);
		CommandLineParser parser = new DefaultParser();
		try {
			CommandLine cli = parser.parse(options, args);
			String path = cli.getOptionValue(ZK_NODE_PATH_LONG);
			String propertiesPath = cli.getOptionValue(CONFIG_PATH_LONG);
			String zkServers = cli.getOptionValue(ZK_SERVERS_LONG);
			
			Properties properties = new Properties();
			properties.load(new FileInputStream(new File(propertiesPath)));
			
			new ConfigCreator(zkServers).uploadProperties(path, properties);
		} catch (ParseException e) {
			sLog.error("Parsing error " + e.getMessage());
			HelpFormatter hFormatter = new HelpFormatter();
			hFormatter.printHelp("java -jar mkconf.jar", options);
		} catch (IOException e) {
			sLog.error("Failed to parse properties " + e.getMessage());
		}
	}
}
