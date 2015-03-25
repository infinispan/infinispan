package org.jboss.as.clustering.infinispan.cs.factory;

import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.cache.SingletonStoreConfiguration;
import org.infinispan.configuration.cache.StoreConfiguration;

import java.util.Properties;

@ConfigurationFor(MyCustomStore.class)
public class MyCustomStoreConfiguration implements StoreConfiguration {

   @Override
   public AsyncStoreConfiguration async() {
      return null;
   }

   @Override
   public SingletonStoreConfiguration singletonStore() {
      return null;
   }

   @Override
   public boolean purgeOnStartup() {
      return false;
   }

   @Override
   public boolean fetchPersistentState() {
      return false;
   }

   @Override
   public boolean ignoreModifications() {
      return false;
   }

   @Override
   public boolean preload() {
      return false;
   }

   @Override
   public boolean shared() {
      return false;
   }

   @Override
   public Properties properties() {
      return null;
   }
}
