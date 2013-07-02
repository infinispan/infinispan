package org.infinispan.loaders.jpa.configuration;

import org.infinispan.configuration.cache.AbstractLockSupportStoreConfiguration;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.cache.LegacyConfigurationAdaptor;
import org.infinispan.configuration.cache.LegacyLoaderAdapter;
import org.infinispan.configuration.cache.SingletonStoreConfiguration;
import org.infinispan.loaders.jpa.JpaCacheStoreConfig;
import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.util.TypedProperties;

/**
 *
 * @author <a href="mailto:rtsang@redhat.com">Ray Tsang</a>
 *
 */
@BuiltBy(JpaCacheStoreConfigurationBuilder.class)
public class JpaCacheStoreConfiguration extends AbstractLockSupportStoreConfiguration implements LegacyLoaderAdapter<JpaCacheStoreConfig>{
	final private String persistenceUnitName;
	final private Class<?> entityClass;
	final private long batchSize;

	protected JpaCacheStoreConfiguration(
			String persistenceUnitName,
			Class<?> entityClass,
			long batchSize,
			long lockAcquistionTimeout,
			int lockConcurrencyLevel, boolean purgeOnStartup,
			boolean purgeSynchronously, int purgerThreads,
			boolean fetchPersistentState, boolean ignoreModifications,
			TypedProperties properties, AsyncStoreConfiguration async,
			SingletonStoreConfiguration singletonStore) {
		super(lockAcquistionTimeout, lockConcurrencyLevel, purgeOnStartup,
				purgeSynchronously, purgerThreads, fetchPersistentState,
				ignoreModifications, properties, async, singletonStore);

		this.persistenceUnitName = persistenceUnitName;
		this.entityClass = entityClass;
		this.batchSize = batchSize;
	}

	public String persistenceUnitName() {
		return persistenceUnitName;
	}

	public Class<?> entityClass() {
		return entityClass;
	}

	public long batchSize() {
	   return batchSize;
	}

	@Override
	public JpaCacheStoreConfig adapt() {
		JpaCacheStoreConfig config = new JpaCacheStoreConfig();

		LegacyConfigurationAdaptor.adapt(this, config);
		config.setPersistenceUnitName(persistenceUnitName);
		config.setEntityClass(entityClass);

		return config;
	}


}
