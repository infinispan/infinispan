package org.jboss.as.clustering.infinispan.cs.factory;

import org.infinispan.filter.KeyFilter;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.persistence.spi.InitializationContext;

import java.util.concurrent.Executor;

public class MyCustomStore implements AdvancedLoadWriteStore {

   @Override
   public void process(KeyFilter filter, CacheLoaderTask task, Executor executor, boolean fetchValue, boolean fetchMetadata) {
   }

   @Override
   public int size() {
      return 0;
   }

   @Override
   public void clear() {
   }

   @Override
   public void purge(Executor threadPool, PurgeListener listener) {
   }

   @Override
   public void init(InitializationContext ctx) {
   }

   @Override
   public void write(MarshalledEntry entry) {
   }

   @Override
   public boolean delete(Object key) {
      return false;
   }

   @Override
   public MarshalledEntry load(Object key) {
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
