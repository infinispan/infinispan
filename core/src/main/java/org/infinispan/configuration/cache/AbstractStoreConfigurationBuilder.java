package org.infinispan.configuration.cache;

import static org.infinispan.configuration.cache.AbstractStoreConfiguration.*;

import java.lang.reflect.Method;
import java.util.Properties;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.util.ReflectionUtil;
import org.infinispan.commons.util.TypedProperties;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.parsing.XmlConfigHelper;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

public abstract class AbstractStoreConfigurationBuilder<T extends StoreConfiguration, S extends AbstractStoreConfigurationBuilder<T, S>>
      extends AbstractPersistenceConfigurationChildBuilder implements StoreConfigurationBuilder<T, S> {

   private static final Log log = LogFactory.getLog(AbstractStoreConfigurationBuilder.class);

   protected final AttributeSet attributes;
   protected final AsyncStoreConfigurationBuilder<S> async;
   protected final SingletonStoreConfigurationBuilder<S> singletonStore;

   @Deprecated
   protected boolean preload;
   @Deprecated
   protected boolean shared;
   @Deprecated
   protected boolean ignoreModifications;
   @Deprecated
   protected Properties properties;
   @Deprecated
   protected boolean purgeOnStartup;
   @Deprecated
   protected boolean fetchPersistentState;

   /**
    * @deprecated Use {@link AbstractStoreConfigurationBuilder#AbstractStoreConfigurationBuilder(PersistenceConfigurationBuilder, AttributeSet)} instead
    */
   @Deprecated
   public AbstractStoreConfigurationBuilder(PersistenceConfigurationBuilder builder) {
      super(builder);
      this.attributes = AbstractStoreConfiguration.attributeDefinitionSet();
      this.async = new AsyncStoreConfigurationBuilder(this);
      this.singletonStore = new SingletonStoreConfigurationBuilder(this);
      initCompatibilitySettings();
   }

   public AbstractStoreConfigurationBuilder(PersistenceConfigurationBuilder builder, AttributeSet attributes) {
      super(builder);
      this.attributes = attributes;
      this.async = new AsyncStoreConfigurationBuilder(this);
      this.singletonStore = new SingletonStoreConfigurationBuilder(this);
      initCompatibilitySettings();
   }

   @Deprecated
   private void initCompatibilitySettings() {
      fetchPersistentState = attributes.attribute(FETCH_PERSISTENT_STATE).get();
      preload = attributes.attribute(PRELOAD).get();
      purgeOnStartup = attributes.attribute(PURGE_ON_STARTUP).get();
      shared = attributes.attribute(SHARED).get();
      ignoreModifications = attributes.attribute(IGNORE_MODIFICATIONS).get();
      properties = attributes.attribute(PROPERTIES).get();
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
   public SingletonStoreConfigurationBuilder<S> singleton() {
      return singletonStore;
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public S fetchPersistentState(boolean b) {
      attributes.attribute(FETCH_PERSISTENT_STATE).set(b);
      fetchPersistentState = b;
      return self();
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public S ignoreModifications(boolean b) {
      attributes.attribute(IGNORE_MODIFICATIONS).set(b);
      ignoreModifications = b;
      return self();
   }

   /**
    * If true, purges this cache store when it starts up.
    */
   @Override
   public S purgeOnStartup(boolean b) {
      attributes.attribute(PURGE_ON_STARTUP).set(b);
      purgeOnStartup = b;
      return self();
   }

   public S properties(Properties properties) {
      attributes.attribute(PROPERTIES).set(new TypedProperties(properties));
      this.properties = properties;
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
      this.properties = properties;
      return self();
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public S withProperties(Properties props) {
      XmlConfigHelper.showUnrecognizedAttributes(XmlConfigHelper.setAttributes(attributes, props, false, false));
      attributes.attribute(PROPERTIES).set(new TypedProperties(props));
      this.properties = props;
      return self();
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public S preload(boolean b) {
      attributes.attribute(PRELOAD).set(b);
      preload = b;
      return self();
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public S shared(boolean b) {
      attributes.attribute(SHARED).set(b);
      shared = b;
      return self();
   }

   @Override
   public void validate() {
      async.validate();
      singletonStore.validate();
      boolean shared = attributes.attribute(SHARED).get();
      boolean fetchPersistentState = attributes.attribute(FETCH_PERSISTENT_STATE).get();
      boolean purgeOnStartup = attributes.attribute(PURGE_ON_STARTUP).get();
      boolean preload = attributes.attribute(PRELOAD).get();
      ConfigurationBuilder builder = getBuilder();
      if (!shared && !fetchPersistentState && !purgeOnStartup
            && builder.clustering().cacheMode().isClustered())
         log.staleEntriesWithoutFetchPersistentStateOrPurgeOnStartup();

      if (shared && !preload && builder.indexing().enabled()
            && builder.indexing().indexLocalOnly())
         log.localIndexingWithSharedCacheLoaderRequiresPreload();
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
      initCompatibilitySettings();
      async.read(template.async());
      singletonStore.read(template.singletonStore());

      return this;
   }

   @Override
   public String toString() {
      return "AbstractStoreConfigurationBuilder [attributes=" + attributes + ", async=" + async + ", singletonStore="
            + singletonStore + "]";
   }
}
