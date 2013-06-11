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

import org.infinispan.configuration.Builder;
import org.infinispan.configuration.cache.AbstractLockSupportStoreConfigurationBuilder;
import org.infinispan.configuration.cache.LoadersConfigurationBuilder;
import org.infinispan.loaders.leveldb.LevelDBCacheStoreConfig;
import org.infinispan.util.TypedProperties;
import org.iq80.leveldb.CompressionType;

/**
 * 
 * @author <a href="mailto:rtsang@redhat.com">Ray Tsang</a>
 * 
 */
public class LevelDBCacheStoreConfigurationBuilder
		extends
		AbstractLockSupportStoreConfigurationBuilder<LevelDBCacheStoreConfiguration, LevelDBCacheStoreConfigurationBuilder> {
   
   protected String location = LevelDBCacheStoreConfig.DEFAULT_LOCATION;
   protected String expiredLocation = LevelDBCacheStoreConfig.DEFAULT_EXPIRED_LOCATION;
   protected CompressionType compressionType = LevelDBCacheStoreConfig.DEFAULT_COMPRESSION_TYPE;
   protected Integer blockSize;
   protected Long cacheSize;
   
   protected int expiryQueueSize = LevelDBCacheStoreConfig.DEFAULT_EXPIRY_QUEUE_SIZE;
   protected int clearThreshold = LevelDBCacheStoreConfig.DEFAULT_CLEAR_THRESHOLD;

	public LevelDBCacheStoreConfigurationBuilder(LoadersConfigurationBuilder builder) {
		super(builder);
	}
	
	public LevelDBCacheStoreConfigurationBuilder location(String location) {
		this.location = location;
		return self();
	}
	
	public LevelDBCacheStoreConfigurationBuilder expiredLocation(String expiredLocation) {
      this.expiredLocation = expiredLocation;
      return self();
   }
	
	public LevelDBCacheStoreConfigurationBuilder blockSize(int blockSize) {
      this.blockSize = blockSize;
      return self();
   }
	
	public LevelDBCacheStoreConfigurationBuilder cacheSize(long cacheSize) {
      this.cacheSize = cacheSize;
      return self();
   }
	
	public LevelDBCacheStoreConfigurationBuilder expiryQueueSize(int expiryQueueSize) {
      this.expiryQueueSize = expiryQueueSize;
      return self();
   }
	
	public LevelDBCacheStoreConfigurationBuilder clearThreshold(int clearThreshold) {
      this.clearThreshold = clearThreshold;
      return self();
   }
	
	public LevelDBCacheStoreConfigurationBuilder compressionType(CompressionType compressionType) {
      this.compressionType = compressionType;
      return self();
   }

	@Override
	public void validate() {
		// how do you validate required attributes?
		super.validate();
	}

	@Override
	public LevelDBCacheStoreConfiguration create() {
		return new LevelDBCacheStoreConfiguration(location, expiredLocation, compressionType, blockSize, 
		      cacheSize, expiryQueueSize, clearThreshold,
				lockAcquistionTimeout, lockConcurrencyLevel, purgeOnStartup,
				purgeSynchronously, purgerThreads, fetchPersistentState,
				ignoreModifications,
				TypedProperties.toTypedProperties(properties), async.create(),
				singletonStore.create());
	}

	@Override
	public Builder<?> read(LevelDBCacheStoreConfiguration template) {
	   location = template.location();
	   expiredLocation = template.expiredLocation();
	   compressionType = template.compressionType();
	   blockSize = template.blockSize();
	   cacheSize = template.cacheSize();
	   
	   expiryQueueSize = template.expiryQueueSize();
	   clearThreshold = template.clearThreshold();

		// LockSupportStore-specific configuration
		lockAcquistionTimeout = template.lockAcquistionTimeout();
		lockConcurrencyLevel = template.lockConcurrencyLevel();

		// AbstractStore-specific configuration
		fetchPersistentState = template.fetchPersistentState();
		ignoreModifications = template.ignoreModifications();
		properties = template.properties();
		purgeOnStartup = template.purgeOnStartup();
		purgeSynchronously = template.purgeSynchronously();
		this.async.read(template.async());
		this.singletonStore.read(template.singletonStore());
		
		return self();
	}

	@Override
	public LevelDBCacheStoreConfigurationBuilder self() {
		return this;
	}

}
