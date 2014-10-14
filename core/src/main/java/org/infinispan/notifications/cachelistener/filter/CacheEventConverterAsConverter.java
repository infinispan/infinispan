package org.infinispan.notifications.cachelistener.filter;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.Util;
import org.infinispan.filter.Converter;
import org.infinispan.filter.KeyFilterAsKeyValueFilter;
import org.infinispan.marshall.core.Ids;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.event.Event;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.Set;

/**
 * Converter that is implemented by using the provided CacheEventConverter.  The provided event type will always be
 * one that is not retried, post and of type CREATE,  The old value and old metadata in both pre and post events will
 * be the data that was in the cache before the event occurs.  The new value and new metadata in both pre and post
 * events will be the data that is in the cache after the event occurs.
 *
 * @author wburns
 * @since 7.0
 */
public class CacheEventConverterAsConverter<K, V, C> implements Converter<K, V, C> {
   private static final EventType CREATE_EVENT = new EventType(false, false, Event.Type.CACHE_ENTRY_CREATED);

   private final CacheEventConverter<K, V, C> converter;

   public CacheEventConverterAsConverter(CacheEventConverter<K, V, C> converter) {
      this.converter = converter;
   }

   @Override
   public C convert(K key, V value, Metadata metadata) {
      return converter.convert(key, null, null, value, metadata, CREATE_EVENT);
   }

   public static class Externalizer extends AbstractExternalizer<CacheEventConverterAsConverter> {
      @Override
      public Set<Class<? extends CacheEventConverterAsConverter>> getTypeClasses() {
         return Util.<Class<? extends CacheEventConverterAsConverter>>asSet(CacheEventConverterAsConverter.class);
      }

      @Override
      public void writeObject(ObjectOutput output, CacheEventConverterAsConverter object) throws IOException {
         output.writeObject(object.converter);
      }

      @Override
      public CacheEventConverterAsConverter readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new CacheEventConverterAsConverter((CacheEventConverter)input.readObject());
      }

      @Override
      public Integer getId() {
         return Ids.CACHE_EVENT_CONVERTER_AS_CONVERTER;
      }
   }
}
