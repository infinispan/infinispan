package org.jboss.as.clustering.infinispan.subsystem;

import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.persistence.spi.AdvancedCacheLoader;
import org.infinispan.persistence.spi.InitializationContext;

import java.util.concurrent.Executor;

public class CustomCacheLoader implements AdvancedCacheLoader {

   @Override
   public void process(KeyFilter filter, CacheLoaderTask cacheLoaderTask, Executor executor, boolean b, boolean b2) {
   }

   @Override
   public int size() {
      return 0;
   }

   @Override
   public void init(InitializationContext initializationContext) {
   }

   @Override
   public MarshalledEntry load(Object o) {
      return null;
   }

   @Override
   public boolean contains(Object o) {
      return false;
   }

   @Override
   public void start() {
   }

   @Override
   public void stop() {
   }
}
