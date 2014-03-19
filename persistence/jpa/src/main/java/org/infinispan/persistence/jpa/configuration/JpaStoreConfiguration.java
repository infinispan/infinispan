package org.infinispan.persistence.jpa.configuration;

import java.util.Properties;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.configuration.cache.AbstractStoreConfiguration;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.cache.SingletonStoreConfiguration;
import org.infinispan.persistence.jpa.JpaStore;

/**
 *
 * @author <a href="mailto:rtsang@redhat.com">Ray Tsang</a>
 *
 */
@BuiltBy(JpaStoreConfigurationBuilder.class)
@ConfigurationFor(JpaStore.class)
public class JpaStoreConfiguration extends AbstractStoreConfiguration {
   final private String persistenceUnitName;
   final private Class<?> entityClass;
   final private long batchSize;
   final private boolean storeMetadata;

   protected JpaStoreConfiguration(boolean purgeOnStartup, boolean fetchPersistentState,
                                   boolean ignoreModifications, AsyncStoreConfiguration async,
                                   SingletonStoreConfiguration singletonStore, boolean preload, boolean shared,
                                   Properties properties,
                                   String persistenceUnitName, Class<?> entityClass,
                                   long batchSize, boolean storeMetadata
   ) {
      super(purgeOnStartup, fetchPersistentState, ignoreModifications, async, singletonStore, preload, shared, properties);
      this.persistenceUnitName = persistenceUnitName;
      this.entityClass = entityClass;
      this.batchSize = batchSize;
      this.storeMetadata = storeMetadata;
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

   public boolean storeMetadata() {
      return storeMetadata;
   }
}
