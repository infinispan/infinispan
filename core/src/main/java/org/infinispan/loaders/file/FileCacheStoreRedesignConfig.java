/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
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

package org.infinispan.loaders.file;

import org.infinispan.loaders.LockSupportCacheStoreConfig;
import org.infinispan.loaders.file.FileCacheStoreConfig.FsyncMode;

/**
 * Configures {@link org.infinispan.loaders.file.FileCacheStoreRedesign}.  This allows you to tune a number of characteristics
 * of the {@link FileCacheStoreRedesign}.
 * <p/>
 *    <ul>
 *       <li><tt>location</tt> - a location on disk where the store can write internal files.  This defaults to
 * <tt>Infinispan-FileCacheStore</tt> in the current working directory.</li>
 *       <li><tt>numberFiles</tt> - Maximum number of files that can be used to store cache data</li>
 * <li><tt>maxSizePerFile</tt> - Maximum size(In Megabyte) per cache file</li>
 * <li><tt>loadThreshold</tt> - Compaction is triggered as soon as a file load becomes less than that threshold</li>
 * </ul>
 *
 * @author Patrick Azogni
 */
public class FileCacheStoreRedesignConfig extends LockSupportCacheStoreConfig{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private String location = "Infinispan-FileCacheStore";
	private int numberFiles = 256;
	private long maxSizePerFile = 512; // in Megabytes
	private float loadThreshold = (float) 0.6; //threshold
	
	
	public String getLocation() {
		return location;
	}
	public void setLocation(String location) {
		this.location = location;
	}

	public static long getSerialversionuid() {
		return serialVersionUID;
	}
	public int getNumberFiles() {
		return numberFiles;
	}
	public void setNumberFiles(int numberFiles) {
		this.numberFiles = numberFiles;
	}
	public long getMaxSizePerFile() {
		return maxSizePerFile;
	}
	public void setMaxFilePerFile(long maxSizePerFile) {
		this.maxSizePerFile = maxSizePerFile;
	}
	public float getLoadThreshold() {
		return loadThreshold;
	}
	public void setLoadTheshold(float loadThreshold) {
		this.loadThreshold = loadThreshold;
	}
	public void setMaxSizePerFile(long maxSizePerFile) {
		this.maxSizePerFile = maxSizePerFile;
	}
	
	

}
