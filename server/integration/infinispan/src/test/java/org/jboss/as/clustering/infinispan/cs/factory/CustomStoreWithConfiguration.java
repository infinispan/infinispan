package org.jboss.as.clustering.infinispan.cs.factory;

import org.infinispan.commons.configuration.ConfiguredBy;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.AdvancedCacheLoader;
import org.infinispan.persistence.spi.InitializationContext;

@ConfiguredBy(CustomStoreConfigurationWithoutBuilder.class)
public class CustomStoreWithConfiguration implements AdvancedCacheLoader<Object,Object> {
   @Override
   public int size() {
      return 0;
   }

   @Override
   public void init(InitializationContext ctx) {
   }

   @Override
   public MarshallableEntry<Object, Object> loadEntry(Object key) {
      return null;
   }

   @Override
   public boolean contains(Object key) {
      return false;
   }

   @Override
   public void start() {
   }

   @Override
   public void stop() {
   }
}
