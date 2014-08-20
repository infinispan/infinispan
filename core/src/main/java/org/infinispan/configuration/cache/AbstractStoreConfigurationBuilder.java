package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.parsing.XmlConfigHelper;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Properties;

public abstract class AbstractStoreConfigurationBuilder<T extends StoreConfiguration, S extends AbstractStoreConfigurationBuilder<T, S>>
      extends AbstractPersistenceConfigurationChildBuilder implements StoreConfigurationBuilder<T, S> {

   private static final Log log = LogFactory.getLog(AbstractStoreConfigurationBuilder.class);

   protected final AsyncStoreConfigurationBuilder<S> async;
   protected final SingletonStoreConfigurationBuilder<S> singletonStore;
   protected boolean fetchPersistentState = false;
   protected boolean ignoreModifications = false;
   protected boolean purgeOnStartup = false;
   protected boolean shared = false;
   protected boolean preload = false;
   protected Properties properties = new Properties();

   public AbstractStoreConfigurationBuilder(PersistenceConfigurationBuilder builder) {
      super(builder);
      this.async = new AsyncStoreConfigurationBuilder(this);
      this.singletonStore = new SingletonStoreConfigurationBuilder(this);
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
      this.fetchPersistentState = b;
      return self();
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public S ignoreModifications(boolean b) {
      this.ignoreModifications = b;
      return self();
   }

   /**
    * If true, purges this cache store when it starts up.
    */
   @Override
   public S purgeOnStartup(boolean b) {
      this.purgeOnStartup = b;
      return self();
   }

   public S properties(Properties properties) {
      this.properties = properties;
      return self();
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public S addProperty(String key, String value) {
      this.properties.put(key, value);
      XmlConfigHelper.setValues(this, properties, false, false);
      return self();
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public S withProperties(Properties props) {
      XmlConfigHelper.showUnrecognizedAttributes(XmlConfigHelper.setValues(this, props, false, false));
      this.properties = props;
      return self();
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public S preload(boolean b) {
      this.preload = b;
      return self();
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public S shared(boolean b) {
      this.shared = b;
      return self();
   }

   @Override
   public void validate() {
      async.validate();
      singletonStore.validate();
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
      async.read(template.async());
      singletonStore.read(template.singletonStore());
      fetchPersistentState = template.fetchPersistentState();
      ignoreModifications = template.ignoreModifications();
      purgeOnStartup = template.purgeOnStartup();
      shared = template.shared();
      preload = template.preload();
      properties = template.properties();

      return this;
   }
}
