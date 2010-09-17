package org.infinispan.loaders.cassandra;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.FileUtils;

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
			cleanDir(s);
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

	/**
	 * Removes all directory content from file the system
	 * 
	 * @param dir
	 * @throws IOException
	 */
	private void cleanDir(String dir) throws IOException {
		File dirFile = new File(dir);
		if (dirFile.exists() && dirFile.isDirectory()) {
			deleteDir(dirFile);
		}
	}

	public static void deleteDir(File dir) throws IOException {
		if (dir.isDirectory()) {
			String[] children = dir.list();
			for (int i = 0; i < children.length; i++) {
				deleteDir(new File(dir, children[i]));
			}
		}

		dir.delete();
	}

}
