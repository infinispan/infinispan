package org.infinispan.persistence.remote.upgrade;

import java.io.ObjectInput;
import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.marshall.exts.NoStateExternalizer;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilter;
import org.infinispan.notifications.cachelistener.filter.EventType;

public class RemovedFilter<K, V> implements CacheEventFilter<K, V> {

   @Override
   public boolean accept(Object key, Object oldValue, Metadata oldMetadata, Object newValue, Metadata newMetadata,
                         EventType eventType) {
      return eventType.isRemove();
   }

   @Override
   public MediaType format() {
      return null;
   }

   public static class Externalizer extends NoStateExternalizer<RemovedFilter> {

      @Override
      public Set<Class<? extends RemovedFilter>> getTypeClasses() {
         return Collections.singleton(RemovedFilter.class);
      }

      @Override
      public RemovedFilter readObject(ObjectInput input) {
         return new RemovedFilter();
      }
   }
}
