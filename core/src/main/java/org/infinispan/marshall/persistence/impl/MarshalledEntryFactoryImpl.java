package org.infinispan.marshall.persistence.impl;

import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.metadata.Metadata;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.MarshallableEntryFactory;
import org.infinispan.persistence.spi.MarshalledValue;

/**
 * @author Ryan Emerson
 * @since 10.0
 */
@Scope(Scopes.NAMED_CACHE)
public class MarshalledEntryFactoryImpl implements MarshallableEntryFactory {

   private static final MarshallableEntry EMPTY = new MarshallableEntryImpl(null, null, (ByteBuffer) null, -1, -1, null);

   @Inject @ComponentName(KnownComponentNames.PERSISTENCE_MARSHALLER)
   Marshaller marshaller;

   public MarshalledEntryFactoryImpl() {
   }

   public MarshalledEntryFactoryImpl(Marshaller marshaller) {
      this.marshaller = marshaller;
   }

   @Override
   public MarshallableEntry create(ByteBuffer key, ByteBuffer valueBytes) {
      return create(key, valueBytes, (ByteBuffer) null, -1, -1);
   }

   @Override
   public MarshallableEntry create(ByteBuffer key, ByteBuffer valueBytes, ByteBuffer metadataBytes, long created, long lastUsed) {
      return new MarshallableEntryImpl<>(key, valueBytes, metadataBytes, created, lastUsed, marshaller);
   }

   @Override
   public MarshallableEntry create(Object key, ByteBuffer valueBytes, ByteBuffer metadataBytes, long created, long lastUsed) {
      return new MarshallableEntryImpl<>(key, valueBytes, metadataBytes, created, lastUsed, marshaller);
   }

   @Override
   public MarshallableEntry create(Object key) {
      return create(key, MarshalledValueImpl.EMPTY);
   }

   @Override
   public MarshallableEntry create(Object key, Object value) {
      return create(key, value, null, -1, -1);
   }

   @Override
   public MarshallableEntry create(Object key, Object value, Metadata metadata, long created, long lastUsed) {
      return new MarshallableEntryImpl<>(key, value, metadata, created, lastUsed, marshaller);
   }

   @Override
   public MarshallableEntry create(Object key, MarshalledValue value) {
      return new MarshallableEntryImpl<>(key, value.getValueBytes(), value.getMetadataBytes(), value.getCreated(), value.getLastUsed(), marshaller);
   }

   @Override
   public MarshallableEntry getEmpty() {
      return EMPTY;
   }
}
