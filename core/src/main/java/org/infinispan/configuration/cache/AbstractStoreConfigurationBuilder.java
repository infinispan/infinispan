package org.infinispan.configuration.cache;

import static org.infinispan.configuration.cache.AbstractStoreConfiguration.FETCH_PERSISTENT_STATE;
import static org.infinispan.configuration.cache.AbstractStoreConfiguration.IGNORE_MODIFICATIONS;
import static org.infinispan.configuration.cache.AbstractStoreConfiguration.MAX_BATCH_SIZE;
import static org.infinispan.configuration.cache.AbstractStoreConfiguration.PRELOAD;
import static org.infinispan.configuration.cache.AbstractStoreConfiguration.PROPERTIES;
import static org.infinispan.configuration.cache.AbstractStoreConfiguration.PURGE_ON_STARTUP;
import static org.infinispan.configuration.cache.AbstractStoreConfiguration.SEGMENTED;
import static org.infinispan.configuration.cache.AbstractStoreConfiguration.SHARED;
import static org.infinispan.configuration.cache.AbstractStoreConfiguration.TRANSACTIONAL;
import static org.infinispan.configuration.cache.AbstractStoreConfiguration.WRITE_ONLY;
import static org.infinispan.util.logging.Log.CONFIG;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.ConfigurationBuilderInfo;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.persistence.Store;
import org.infinispan.commons.util.ReflectionUtil;
import org.infinispan.commons.util.TypedProperties;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.parsing.XmlConfigHelper;
import org.infinispan.persistence.spi.NonBlockingStore;
import org.infinispan.persistence.spi.SegmentedAdvancedLoadWriteStore;

public abstract class AbstractStoreConfigurationBuilder<T extends StoreConfiguration, S extends AbstractStoreConfigurationBuilder<T, S>>
      extends AbstractPersistenceConfigurationChildBuilder implements StoreConfigurationBuilder<T, S>, ConfigurationBuilderInfo {

   protected final AttributeSet attributes;
   protected final AsyncStoreConfigurationBuilder<S> async;

   private final List<ConfigurationBuilderInfo> subElements = new ArrayList<>();

   public AbstractStoreConfigurationBuilder(PersistenceConfigurationBuilder builder, AttributeSet attributes) {
      super(builder);
      this.attributes = attributes;
      this.async = new AsyncStoreConfigurationBuilder(this);
      subElements.add(async);
   }

   public AbstractStoreConfigurationBuilder(PersistenceConfigurationBuilder builder, AttributeSet attributes,
         AttributeSet asyncAttributeSet) {
      super(builder);
      this.attributes = attributes;
      this.async = new AsyncStoreConfigurationBuilder(this, asyncAttributeSet);
      subElements.add(async);
   }

   @Override
   public Collection<ConfigurationBuilderInfo> getChildrenInfo() {
      return subElements;
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
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
    */
   @Override
   public S fetchPersistentState(boolean b) {
      attributes.attribute(FETCH_PERSISTENT_STATE).set(b);
      return self();
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public S ignoreModifications(boolean b) {
      attributes.attribute(IGNORE_MODIFICATIONS).set(b);
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
      XmlConfigHelper.setAttributes(attributes, properties, false, false);
      return self();
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public S withProperties(Properties props) {
      XmlConfigHelper.showUnrecognizedAttributes(XmlConfigHelper.setAttributes(attributes, props, false, false));
      attributes.attribute(PROPERTIES).set(new TypedProperties(props));
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

   @Override
   public void validate() {
      validate(false);
   }

   protected void validate(boolean skipClassChecks) {
      if (!skipClassChecks)
         validateStoreWithAnnotations();
      validateStoreAttributes();
   }

   private void validateStoreAttributes() {
      async.validate();
      boolean shared = attributes.attribute(SHARED).get();
      boolean preload = attributes.attribute(PRELOAD).get();
      boolean fetchPersistentState = attributes.attribute(FETCH_PERSISTENT_STATE).get();
      boolean purgeOnStartup = attributes.attribute(PURGE_ON_STARTUP).get();
      boolean transactional = attributes.attribute(TRANSACTIONAL).get();
      boolean readOnly = attributes.attribute(IGNORE_MODIFICATIONS).get();
      boolean writeOnly = attributes.attribute(WRITE_ONLY).get();
      ConfigurationBuilder builder = getBuilder();

      if (purgeOnStartup && preload) {
         throw CONFIG.preloadAndPurgeOnStartupConflict();
      }

      if (readOnly && writeOnly) {
         throw CONFIG.storeBothReadAndWriteOnly();
      }

      if (readOnly && (purgeOnStartup || shared)) {
         throw CONFIG.storeReadOnlyExceptions();
      }

      if (writeOnly && (fetchPersistentState || preload)) {
         throw CONFIG.storeWriteOnlyExceptions();
      }

      if (!shared && !fetchPersistentState && !purgeOnStartup
            && builder.clustering().cacheMode().isClustered() && !getBuilder().template())
         CONFIG.staleEntriesWithoutFetchPersistentStateOrPurgeOnStartup();

      if (fetchPersistentState && attributes.attribute(FETCH_PERSISTENT_STATE).isModified() &&
            clustering().cacheMode().isInvalidation()) {
         throw CONFIG.attributeNotAllowedInInvalidationMode(FETCH_PERSISTENT_STATE.name());
      }

      if (shared && !builder.clustering().cacheMode().isClustered()) {
         throw CONFIG.sharedStoreWithLocalCache();
      }

      if (transactional && !builder.transaction().transactionMode().isTransactional())
         throw CONFIG.transactionalStoreInNonTransactionalCache();

      if (transactional && builder.persistence().passivation())
         throw CONFIG.transactionalStoreInPassivatedCache();
   }

   private void validateStoreWithAnnotations() {
      Class configKlass = attributes.getKlass();
      if (configKlass != null && configKlass.isAnnotationPresent(ConfigurationFor.class)) {
         Class storeKlass = ((ConfigurationFor) configKlass.getAnnotation(ConfigurationFor.class)).value();
         if (storeKlass.isAnnotationPresent(Store.class)) {
            Store storeProps = (Store) storeKlass.getAnnotation(Store.class);
            boolean segmented = attributes.attribute(SEGMENTED).get();
            if (segmented && !AbstractSegmentedStoreConfiguration.class.isAssignableFrom(configKlass)
                  && !(SegmentedAdvancedLoadWriteStore.class.isAssignableFrom(storeKlass) || NonBlockingStore.class.isAssignableFrom(storeKlass))) {
               throw CONFIG.storeNotSegmented(storeKlass);
            }
            if (!storeProps.shared() && attributes.attribute(SHARED).get()) {
               throw CONFIG.nonSharedStoreConfiguredAsShared(storeKlass.getSimpleName());
            }
         }
      } else {
         CONFIG.warnConfigurationForAnnotationMissing(attributes.getName());
      }
   }

   @Override
   public void validate(GlobalConfiguration globalConfig) {
   }

   @Override
   public Builder<?> read(T template) {

      Method attributesMethod = ReflectionUtil.findMethod(template.getClass(), "attributes");
      try {
         attributes.read((AttributeSet) attributesMethod.invoke(template, null));
      } catch (Exception e) {
         throw new CacheConfigurationException(e);
      }
      async.read(template.async());

      return this;
   }

   @Override
   public String toString() {
      return "AbstractStoreConfigurationBuilder [attributes=" + attributes + ", async=" + async + "]";
   }
}
