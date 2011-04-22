/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
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
