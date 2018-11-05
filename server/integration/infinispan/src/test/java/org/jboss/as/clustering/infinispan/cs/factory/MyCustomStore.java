package org.jboss.as.clustering.infinispan.cs.factory;

import java.util.concurrent.Executor;

import org.infinispan.persistence.spi.MarshalledEntry;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.persistence.spi.InitializationContext;

public class MyCustomStore implements AdvancedLoadWriteStore {
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
