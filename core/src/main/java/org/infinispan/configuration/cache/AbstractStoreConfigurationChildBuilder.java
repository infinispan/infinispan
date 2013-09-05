package org.infinispan.configuration.cache;

import java.util.Properties;

/**
 * @author Mircea Markus
 * @since 6.0
 */
public abstract class AbstractStoreConfigurationChildBuilder<S> extends AbstractPersistenceConfigurationChildBuilder implements StoreConfigurationChildBuilder<S> {

   private final StoreConfigurationBuilder<? extends AbstractStoreConfiguration, ? extends StoreConfigurationBuilder<?,?>> builder;

   protected AbstractStoreConfigurationChildBuilder(StoreConfigurationBuilder<? extends AbstractStoreConfiguration, ? extends StoreConfigurationBuilder<?,?>> builder) {
      super(builder.persistence());
      this.builder = builder;
   }

   @Override
   public AsyncStoreConfigurationBuilder<S> async() {
      return (AsyncStoreConfigurationBuilder<S>) builder.async();
   }

   @Override
   public SingletonStoreConfigurationBuilder<S> singleton() {
      return (SingletonStoreConfigurationBuilder<S>) builder.singleton();
   }

   @Override
   public S fetchPersistentState(boolean b) {
      return (S)builder.fetchPersistentState(b);
   }

   @Override
   public S ignoreModifications(boolean b) {
      return (S)builder.ignoreModifications(b);
   }

   @Override
   public S purgeOnStartup(boolean b) {
      return (S)builder.purgeOnStartup(b);
   }

   @Override
   public S preload(boolean b) {
      return (S)builder.preload(b);
   }

   @Override
   public S shared(boolean b) {
      return (S)builder.shared(b);
   }

   @Override
   public S addProperty(String key, String value) {
      return (S)builder.addProperty(key, value);
   }

   @Override
   public S withProperties(Properties p) {
      return (S)builder.withProperties(p);
   }
}
