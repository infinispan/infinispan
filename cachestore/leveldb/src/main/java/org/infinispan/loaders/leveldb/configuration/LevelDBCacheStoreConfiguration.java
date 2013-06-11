/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
package org.infinispan.loaders.leveldb.configuration;

import org.infinispan.configuration.BuiltBy;
import org.infinispan.configuration.cache.AbstractLockSupportStoreConfiguration;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.cache.LegacyConfigurationAdaptor;
import org.infinispan.configuration.cache.LegacyLoaderAdapter;
import org.infinispan.configuration.cache.SingletonStoreConfiguration;
import org.infinispan.loaders.leveldb.LevelDBCacheStoreConfig;
import org.infinispan.util.TypedProperties;
import org.iq80.leveldb.CompressionType;

/**
 * 
 * @author <a href="mailto:rtsang@redhat.com">Ray Tsang</a>
 *
 */
@BuiltBy(LevelDBCacheStoreConfigurationBuilder.class)
public class LevelDBCacheStoreConfiguration extends AbstractLockSupportStoreConfiguration implements LegacyLoaderAdapter<LevelDBCacheStoreConfig>{
   final private String location;
   final private String expiredLocation;
   final private CompressionType compressionType;
   final private Integer blockSize;
   final private Long cacheSize;
   final private int expiryQueueSize;
   final private int clearThreshold;
	
	protected LevelDBCacheStoreConfiguration(
	      String location,
	      String expiredLocation,
	      CompressionType compressionType,
	      Integer blockSize,
	      Long cacheSize,
	      int expiryQueueSize,
	      int clearThreshold,
			long lockAcquistionTimeout,
			int lockConcurrencyLevel, boolean purgeOnStartup,
			boolean purgeSynchronously, int purgerThreads,
			boolean fetchPersistentState, boolean ignoreModifications,
			TypedProperties properties, AsyncStoreConfiguration async,
			SingletonStoreConfiguration singletonStore) {
		super(lockAcquistionTimeout, lockConcurrencyLevel, purgeOnStartup,
				purgeSynchronously, purgerThreads, fetchPersistentState,
				ignoreModifications, properties, async, singletonStore);
		
		this.location = location;
		this.expiredLocation = expiredLocation;
		this.compressionType = compressionType;
		this.blockSize = blockSize;
		this.cacheSize = cacheSize;
		this.expiryQueueSize = expiryQueueSize;
		this.clearThreshold = clearThreshold;
	}

	@Override
	public LevelDBCacheStoreConfig adapt() {
	   LevelDBCacheStoreConfig config = new LevelDBCacheStoreConfig();
		
		LegacyConfigurationAdaptor.adapt(this, config);
		config.setLocation(location);
		config.setExpiredLocation(expiredLocation);
		config.setCompressionType(compressionType.toString());
		config.setBlockSize(blockSize);
		config.setCacheSize(cacheSize);
		config.setExpiryQueueSize(expiryQueueSize);
		config.setClearThreshold(clearThreshold);
		
		return config;
	}
	
   public String location() {
      return location;
   }
   
   public String expiredLocation() {
      return expiredLocation;
   }
   
   public CompressionType compressionType() {
      return compressionType;
   }
   
   public Integer blockSize() {
      return blockSize;
   }
   
   public Long cacheSize() {
      return cacheSize;
   }

   public int expiryQueueSize() {
      return expiryQueueSize;
   }

   public int clearThreshold() {
      return clearThreshold;
   }
}
