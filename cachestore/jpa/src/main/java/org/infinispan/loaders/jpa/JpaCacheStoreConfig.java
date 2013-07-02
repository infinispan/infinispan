package org.infinispan.loaders.jpa;

import java.util.Properties;

import org.infinispan.loaders.LockSupportCacheStoreConfig;
import org.infinispan.commons.util.TypedProperties;

/**
 *
 * @author <a href="mailto:rtsang@redhat.com">Ray Tsang</a>
 *
 */
public class JpaCacheStoreConfig extends LockSupportCacheStoreConfig {
	private static final long serialVersionUID = -8588748696540797858L;

	public static final long DEFAULT_BATCH_SIZE = 100L;

	private String persistenceUnitName;
   private String entityClassName;
   private Class<?> entityClass;
   private long batchSize = DEFAULT_BATCH_SIZE;

	public JpaCacheStoreConfig() {
		cacheLoaderClassName = JpaCacheStore.class.getName();
	}

	protected JpaCacheStoreConfig(JpaCacheStoreConfig config) {
		Properties p = this.getProperties();
		setProperty(config.getPersistenceUnitName(), "persistenceUnitName", p);
		setProperty(config.getEntityClassName(), "entityClassName", p);
		setProperty(String.valueOf(config.getBatchSize()), "batchSize", p);
	}

	public String getPersistenceUnitName() {
		return persistenceUnitName;
	}

	public void setPersistenceUnitName(String persistenceUnitName) {
		this.persistenceUnitName = persistenceUnitName;
	}

	public String getEntityClassName() {
		return entityClassName;
	}

	public void setEntityClassName(String entityClassName)
			throws ClassNotFoundException {
	   this.entityClassName = entityClassName;
	   entityClass = this.getClass().getClassLoader()
            .loadClass(entityClassName);
	}

	public Class<?> getEntityClass() {
		return entityClass;
	}

	public void setEntityClass(Class<?> entityClass) {
		this.entityClass = entityClass;
		this.entityClassName = entityClass.getName();
	}

	public long getBatchSize() {
		return batchSize;
	}

	public void setBatchSize(long batchSize) {
		this.batchSize = batchSize;
	}

	protected void setProperty(String properyValue, String propertyName,
			Properties p) {
		if (properyValue != null) {
			try {
				p.setProperty(propertyName, properyValue);
			} catch (UnsupportedOperationException e) {
				// Most likely immutable, so let's work around that
				TypedProperties writableProperties = new TypedProperties(p);
				writableProperties.setProperty(propertyName, properyValue);
				setProperties(writableProperties);
			}
		}
	}

}
