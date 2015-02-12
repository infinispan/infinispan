package org.infinispan.notifications.cachelistener.filter;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.Util;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.filter.AbstractKeyValueFilterConverter;
import org.infinispan.marshall.core.Ids;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.event.Event;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

/**
 * @author anistor@redhat.com
 * @since 7.2
 */
public class CacheEventFilterConverterAsKeyValueFilterConverter<K, V, C> extends AbstractKeyValueFilterConverter<K, V, C> {

   private static final EventType CREATE_EVENT = new EventType(false, false, Event.Type.CACHE_ENTRY_CREATED);

   private final CacheEventFilterConverter<K, V, C> cacheEventFilterConverter;

   public CacheEventFilterConverterAsKeyValueFilterConverter(CacheEventFilterConverter<K, V, C> cacheEventFilterConverter) {
      this.cacheEventFilterConverter = cacheEventFilterConverter;
   }

   @Override
   public C filterAndConvert(K key, V value, Metadata metadata) {
      return cacheEventFilterConverter.filterAndConvert(key, null, null, value, metadata, CREATE_EVENT);
   }

   @Inject
   protected void injectDependencies(ComponentRegistry cr) {
      cr.wireDependencies(cacheEventFilterConverter);
   }

   public static class Externalizer extends AbstractExternalizer<CacheEventFilterConverterAsKeyValueFilterConverter> {

      @Override
      public Set<Class<? extends CacheEventFilterConverterAsKeyValueFilterConverter>> getTypeClasses() {
         return Util.<Class<? extends CacheEventFilterConverterAsKeyValueFilterConverter>>asSet(CacheEventFilterConverterAsKeyValueFilterConverter.class);
      }

      @Override
      public void writeObject(ObjectOutput output, CacheEventFilterConverterAsKeyValueFilterConverter object) throws IOException {
         output.writeObject(object.cacheEventFilterConverter);
      }

      @Override
      public CacheEventFilterConverterAsKeyValueFilterConverter readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new CacheEventFilterConverterAsKeyValueFilterConverter((CacheEventFilterConverter) input.readObject());
      }

      @Override
      public Integer getId() {
         return Ids.CACHE_EVENT_FILTER_CONVERTER_AS_KEY_VALUE_FILTER_CONVERTER;
      }
   }
}
