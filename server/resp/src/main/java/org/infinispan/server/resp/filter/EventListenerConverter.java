package org.infinispan.server.resp.filter;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.encoding.DataConversion;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverter;
import org.infinispan.notifications.cachelistener.filter.EventType;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

@ProtoTypeId(ProtoStreamTypeIds.RESP_EVENT_LISTENER_CONVERTER)
@Scope(Scopes.NONE)
public class EventListenerConverter<K, V, C> implements CacheEventConverter<K, V, C> {

   @ProtoField(1)
   final DataConversion dc;

   @ProtoFactory
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
}
