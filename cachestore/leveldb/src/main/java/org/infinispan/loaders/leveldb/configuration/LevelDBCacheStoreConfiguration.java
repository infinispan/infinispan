package org.infinispan.loaders.leveldb.configuration;

import org.infinispan.configuration.cache.AbstractLockSupportStoreConfiguration;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.cache.LegacyConfigurationAdaptor;
import org.infinispan.configuration.cache.LegacyLoaderAdapter;
import org.infinispan.configuration.cache.SingletonStoreConfiguration;
import org.infinispan.loaders.leveldb.LevelDBCacheStoreConfig;
import org.infinispan.loaders.leveldb.LevelDBCacheStoreConfig.ImplementationType;
import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.util.TypedProperties;
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
   final private ImplementationType implementationType;
   final private CompressionType compressionType;
   final private Integer blockSize;
   final private Long cacheSize;
   final private int expiryQueueSize;
   final private int clearThreshold;

	protected LevelDBCacheStoreConfiguration(
	      String location,
	      String expiredLocation,
	      ImplementationType implementationType,
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
		this.implementationType = implementationType;
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
		config.setImplementationType(implementationType.toString());
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

   public ImplementationType implementationType() {
      return implementationType;
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
