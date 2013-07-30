package org.infinispan.loaders.jpa.configuration;

import org.infinispan.configuration.cache.AbstractLockSupportStoreConfigurationBuilder;
import org.infinispan.configuration.cache.LoadersConfigurationBuilder;
import org.infinispan.loaders.jpa.JpaCacheStoreConfig;
import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.util.TypedProperties;

/**
 *
 * @author <a href="mailto:rtsang@redhat.com">Ray Tsang</a>
 *
 */
public class JpaCacheStoreConfigurationBuilder
		extends
		AbstractLockSupportStoreConfigurationBuilder<JpaCacheStoreConfiguration, JpaCacheStoreConfigurationBuilder> {

	private String persistenceUnitName;
	private Class<?> entityClass;
	private long batchSize = JpaCacheStoreConfig.DEFAULT_BATCH_SIZE;

	public JpaCacheStoreConfigurationBuilder(LoadersConfigurationBuilder builder) {
		super(builder);
	}

	public JpaCacheStoreConfigurationBuilder persistenceUnitName(String persistenceUnitName) {
		this.persistenceUnitName = persistenceUnitName;
		return self();
	}

	public JpaCacheStoreConfigurationBuilder entityClass(Class<?> entityClass) {
		this.entityClass = entityClass;
		return self();
	}

	public JpaCacheStoreConfigurationBuilder batchSize(long batchSize) {
	   this.batchSize = batchSize;
	   return self();
	}

	@Override
	public void validate() {
		// how do you validate required attributes?
		super.validate();
	}

	@Override
	public JpaCacheStoreConfiguration create() {
		return new JpaCacheStoreConfiguration(persistenceUnitName, entityClass, batchSize,
				lockAcquistionTimeout, lockConcurrencyLevel, purgeOnStartup,
				purgeSynchronously, purgerThreads, fetchPersistentState,
				ignoreModifications,
				TypedProperties.toTypedProperties(properties), async.create(),
				singletonStore.create());
	}

	@Override
	public Builder<?> read(JpaCacheStoreConfiguration template) {
		persistenceUnitName = template.persistenceUnitName();
		entityClass = template.entityClass();
		batchSize = template.batchSize();

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
	public JpaCacheStoreConfigurationBuilder self() {
		return this;
	}

}
