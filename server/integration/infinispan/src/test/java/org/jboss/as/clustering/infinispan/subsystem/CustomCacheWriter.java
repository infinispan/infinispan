package org.jboss.as.clustering.infinispan.subsystem;

import java.util.concurrent.Executor;

import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.AdvancedCacheWriter;
import org.infinispan.persistence.spi.InitializationContext;

public class CustomCacheWriter implements AdvancedCacheWriter {

   @Override
   public void clear() {
   }

   @Override
   public void purge(Executor executor, PurgeListener purgeListener) {
   }

   @Override
   public void init(InitializationContext initializationContext) {
   }

   @Override
   public void write(MarshallableEntry entry) {
   }

   @Override
   public boolean delete(Object o) {
      return false;
   }

   @Override
   public void start() {
   }

   @Override
   public void stop() {
   }
}
