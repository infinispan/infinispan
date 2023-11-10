package org.infinispan.server.resp.filter;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.encoding.DataConversion;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverter;
import org.infinispan.notifications.cachelistener.filter.EventType;
import org.infinispan.server.resp.ExternalizerIds;
@Scope(Scopes.NONE)
public class EventListenerConverter<K, V, C> implements CacheEventConverter<K, V, C> {
   public static AdvancedExternalizer<EventListenerConverter> EXTERNALIZER = new EventListenerConverter.Externalizer();
   private DataConversion dc;

   public EventListenerConverter(DataConversion dc) {
      this.dc = dc;
   }

   @Inject
   public void doWiring(ComponentRegistry registry) {
      registry.wireDependencies(dc);
   }

   @Override
   public C convert(K key, V oldValue, Metadata oldMetadata, V newValue, Metadata newMetadata, EventType eventType) {
      return (C) dc.fromStorage(newValue);
   }

   @Override
   public MediaType format() {
      return MediaType.APPLICATION_OCTET_STREAM;
   }

   private static class Externalizer extends AbstractExternalizer<EventListenerConverter> {

      @Override
      public Set<Class<? extends EventListenerConverter>> getTypeClasses() {
         return Collections.singleton(EventListenerConverter.class);
      }

      @Override
      public void writeObject(ObjectOutput output, EventListenerConverter object) throws IOException {
         output.writeObject(object.dc);
      }

      @Override
      public EventListenerConverter readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new EventListenerConverter((DataConversion)input.readObject());
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.EVENT_LISTENER_CONVERTER;
      }
   }
}
