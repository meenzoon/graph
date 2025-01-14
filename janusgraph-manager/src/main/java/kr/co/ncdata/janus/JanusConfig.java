package kr.co.ncdata.janus;

import java.io.File;

public abstract class JanusConfig {
	private static final String OS_ENV = System.getProperty("os.name").toLowerCase();
	private static final boolean ENV_CONFIG = OS_ENV.contains("win");
	/**
	 * janusgraph config path
	 */
	private static final String CONF_PATH = ENV_CONFIG ? "C:\\tools\\map" : "/home/janus/janusgraph-1.1.0/conf";
	/**
	 * janusgraph properties file name
	 */
	public static final String HBASE_ES_PROP_FILE_NAME = CONF_PATH + File.separator + "janusgraph-hbase-es.properties";
	/**
	 * janusgraph remote properties file name
	 */
	public static final String REMOTE_PROP_FILE_NAME = CONF_PATH + File.separator + "remote-graph.properties";

	/**
	 * 데이터 저장 위치
	 */
	private static final String DATA_PATH = ENV_CONFIG ? "C:\\tools\\map" : "/home/janus/map";

	/**
	 * OSM Node, Way File
	 */
	public static final String OSM_NODE_FILE = DATA_PATH + File.separator + "daegu_node.csv";
	public static final String OSM_WAY_FILE = DATA_PATH + File.separator + "daegu_way.csv";

	/**
	 * 국가표준노드링크 File
	 */
	private static final String ITS_FILE_PATH = DATA_PATH + File.separator + "nodelink";
	public static final String ITS_NODE_FILE = ITS_FILE_PATH + File.separator + "MOCT_NODE.shp";
	public static final String ITS_LINK_FILE = ITS_FILE_PATH + File.separator + "MOCT_LINK.shp";
}
