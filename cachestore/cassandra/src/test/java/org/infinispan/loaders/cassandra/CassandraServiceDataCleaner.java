package org.infinispan.loaders.cassandra;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.FileUtils;
import org.infinispan.test.TestingUtil;

public class CassandraServiceDataCleaner {
	/**
	 * Creates all data dir if they don't exist and cleans them
	 * 
	 * @throws IOException
	 */
	public void prepare() throws IOException {
		makeDirsIfNotExist();
		cleanupDataDirectories();
	}

	/**
	 * Deletes all data from cassandra data directories, including the commit
	 * log.
	 * 
	 * @throws IOException
	 *             in case of permissions error etc.
	 */
	public void cleanupDataDirectories() throws IOException {
		for (String s : getDataDirs()) {
			TestingUtil.recursiveFileRemove(s);
		}
	}

	/**
	 * Creates the data diurectories, if they didn't exist.
	 * 
	 * @throws IOException
	 *             if directories cannot be created (permissions etc).
	 */
	public void makeDirsIfNotExist() throws IOException {
		for (String s : getDataDirs()) {
			mkdir(s);
		}
	}

	/**
	 * Collects all data dirs and returns a set of String paths on the file
	 * system.
	 * 
	 * @return
	 */
	private Set<String> getDataDirs() {
		Set<String> dirs = new HashSet<String>();
		for (String s : DatabaseDescriptor.getAllDataFileLocations()) {
			dirs.add(s);
		}
		dirs.add(DatabaseDescriptor.getLogFileLocation());
		return dirs;
	}

	/**
	 * Creates a directory
	 * 
	 * @param dir
	 * @throws IOException
	 */
	private void mkdir(String dir) throws IOException {
		FileUtils.createDirectory(dir);
	}

}
