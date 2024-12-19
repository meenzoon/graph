package kr.co.ncdata.janus;

import java.io.File;

public abstract class JanusManager {
	private static final String CONF_PATH = System.getProperty("os.name").toLowerCase().contains("win") ? "C:\\tools\\map" : "/home/janus/janusgraph-1.0.0/conf";
	public static final String PROP_FILE_NAME = CONF_PATH + File.separator + "janusgraph-hbase-es.properties";
}
