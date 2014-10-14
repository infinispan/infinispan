package org.infinispan.notifications.cachelistener.filter;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.Util;
import org.infinispan.filter.KeyValueFilter;
import org.infinispan.marshall.core.Ids;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.event.Event;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.Set;

/**
 * KeyValueFilter that is implemented by using the provided CacheEventFilter.  The provided event type will always be
 * one that is not retried, post and of type CREATE,  The old value and old metadata in both pre and post events will
 * be the data that was in the cache before the event occurs.  The new value and new metadata in both pre and post
 * events will be the data that is in the cache after the event occurs.
 *
 * @author wburns
 * @since 7.0
 */
public class CacheEventFilterAsKeyValueFilter<K, V> implements KeyValueFilter<K, V>, Serializable {
   private static final EventType CREATE_EVENT = new EventType(false, false, Event.Type.CACHE_ENTRY_CREATED);

   private final CacheEventFilter<K, V> filter;

   public CacheEventFilterAsKeyValueFilter(CacheEventFilter<K, V> filter) {
      this.filter = filter;
   }

   @Override
   public boolean accept(K key, V value, Metadata metadata) {
      return filter.accept(key, value, metadata, null, null, CREATE_EVENT);
   }

   public static class Externalizer extends AbstractExternalizer<CacheEventFilterAsKeyValueFilter> {
      @Override
      public Set<Class<? extends CacheEventFilterAsKeyValueFilter>> getTypeClasses() {
         return Util.<Class<? extends CacheEventFilterAsKeyValueFilter>>asSet(CacheEventFilterAsKeyValueFilter.class);
      }

      @Override
      public void writeObject(ObjectOutput output, CacheEventFilterAsKeyValueFilter object) throws IOException {
         output.writeObject(object.filter);
      }

      @Override
      public CacheEventFilterAsKeyValueFilter readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new CacheEventFilterAsKeyValueFilter((CacheEventFilter)input.readObject());
      }

      @Override
      public Integer getId() {
         return Ids.CACHE_EVENT_FILTER_AS_KEY_VALUE_FILTER;
      }
   }
}
