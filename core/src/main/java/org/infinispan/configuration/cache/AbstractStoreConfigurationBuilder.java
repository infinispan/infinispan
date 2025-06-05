package org.infinispan.configuration.cache;

import static org.infinispan.configuration.cache.AbstractStoreConfiguration.MAX_BATCH_SIZE;
import static org.infinispan.configuration.cache.AbstractStoreConfiguration.PRELOAD;
import static org.infinispan.configuration.cache.AbstractStoreConfiguration.PROPERTIES;
import static org.infinispan.configuration.cache.AbstractStoreConfiguration.PURGE_ON_STARTUP;
import static org.infinispan.configuration.cache.AbstractStoreConfiguration.READ_ONLY;
import static org.infinispan.configuration.cache.AbstractStoreConfiguration.SEGMENTED;
import static org.infinispan.configuration.cache.AbstractStoreConfiguration.SHARED;
import static org.infinispan.configuration.cache.AbstractStoreConfiguration.TRANSACTIONAL;
import static org.infinispan.configuration.cache.AbstractStoreConfiguration.WRITE_ONLY;
import static org.infinispan.util.logging.Log.CONFIG;

import java.util.Properties;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.util.TypedProperties;

public abstract class AbstractStoreConfigurationBuilder<T extends StoreConfiguration, S extends AbstractStoreConfigurationBuilder<T, S>>
      extends AbstractPersistenceConfigurationChildBuilder implements StoreConfigurationBuilder<T, S> {

   protected final AttributeSet attributes;
   protected final AsyncStoreConfigurationBuilder<S> async;

   public AbstractStoreConfigurationBuilder(PersistenceConfigurationBuilder builder, AttributeSet attributes) {
      super(builder);
      this.attributes = attributes;
      this.async = new AsyncStoreConfigurationBuilder(this);
   }

   public AbstractStoreConfigurationBuilder(PersistenceConfigurationBuilder builder, AttributeSet attributes,
         AttributeSet asyncAttributeSet) {
      super(builder);
      this.attributes = attributes;
      this.async = new AsyncStoreConfigurationBuilder(this, asyncAttributeSet);
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public AsyncStoreConfigurationBuilder<S> async() {
      return async;
   }

   /**
    * {@inheritDoc}
    *
    * @deprecated Deprecated since 14.0. There is no replacement. First non shared store is picked instead.
    */
   @Deprecated(forRemoval=true, since = "14.0")
   @Override
   public S fetchPersistentState(boolean b) {
      return self();
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public S ignoreModifications(boolean b) {
      attributes.attribute(READ_ONLY).set(b);
      return self();
   }

   /**
    * If true, purges this cache store when it starts up.
    */
   @Override
   public S purgeOnStartup(boolean b) {
      attributes.attribute(PURGE_ON_STARTUP).set(b);
      return self();
   }

   @Override
   public S writeOnly(boolean b) {
      attributes.attribute(WRITE_ONLY).set(b);
      return self();
   }

   public S properties(Properties properties) {
      attributes.attribute(PROPERTIES).set(new TypedProperties(properties));
      return self();
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public S addProperty(String key, String value) {
      TypedProperties properties = attributes.attribute(PROPERTIES).get();
      properties.put(key, value);
      attributes.attribute(PROPERTIES).set(properties);
      return self();
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public S withProperties(Properties props) {
      attributes.attribute(PROPERTIES).set(TypedProperties.toTypedProperties(props));
      return self();
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public S preload(boolean b) {
      attributes.attribute(PRELOAD).set(b);
      return self();
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public S shared(boolean b) {
      attributes.attribute(SHARED).set(b);
      return self();
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public S transactional(boolean b) {
      attributes.attribute(TRANSACTIONAL).set(b);
      return self();
   }

   @Override
   public S maxBatchSize(int maxBatchSize) {
      attributes.attribute(MAX_BATCH_SIZE).set(maxBatchSize);
      return self();
   }

   @Override
   public S segmented(boolean b) {
      attributes.attribute(SEGMENTED).set(b);
      return self();
   }

   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public void validate() {
      validateStoreAttributes();
   }

   private void validateStoreAttributes() {
      async.validate();
      boolean shared = attributes.attribute(SHARED).get();
      boolean preload = attributes.attribute(PRELOAD).get();
      boolean purgeOnStartup = attributes.attribute(PURGE_ON_STARTUP).get();
      boolean transactional = attributes.attribute(TRANSACTIONAL).get();
      boolean readOnly = attributes.attribute(READ_ONLY).get();
      boolean writeOnly = attributes.attribute(WRITE_ONLY).get();
      ConfigurationBuilder builder = getBuilder();

      if (purgeOnStartup && preload) {
         throw CONFIG.preloadAndPurgeOnStartupConflict();
      }

      if (readOnly && writeOnly) {
         throw CONFIG.storeBothReadAndWriteOnly();
      }

      if (readOnly && (purgeOnStartup || shared || persistence().passivation())) {
         throw CONFIG.storeReadOnlyExceptions();
      }

      if (writeOnly && preload) {
         throw CONFIG.storeWriteOnlyExceptions();
      }

      if (shared && !builder.clustering().cacheMode().isClustered()) {
         throw CONFIG.sharedStoreWithLocalCache();
      }

      if (transactional && !builder.transaction().transactionMode().isTransactional())
         throw CONFIG.transactionalStoreInNonTransactionalCache();

      if (transactional && builder.persistence().passivation())
         throw CONFIG.transactionalStoreInPassivatedCache();
   }

   @Override
   public Builder<?> read(T template, Combine combine) {
      attributes.read(template.attributes(), combine);
      async.read(template.async(), combine);
      return this;
   }

   @Override
   public String toString() {
      return "AbstractStoreConfigurationBuilder [attributes=" + attributes + ", async=" + async + "]";
   }
}
