package org.infinispan.marshall.persistence.impl;

import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.PrivateMetadata;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.MarshallableEntryFactory;
import org.infinispan.persistence.spi.MarshalledValue;

/**
 * @author Ryan Emerson
 * @since 10.0
 */
@Scope(Scopes.NAMED_CACHE)
public class MarshalledEntryFactoryImpl implements MarshallableEntryFactory {

   private static final MarshallableEntry EMPTY = new MarshallableEntryImpl();

   @Inject @ComponentName(KnownComponentNames.PERSISTENCE_MARSHALLER)
   Marshaller marshaller;

   public MarshalledEntryFactoryImpl() {
   }

   public MarshalledEntryFactoryImpl(Marshaller marshaller) {
      this.marshaller = marshaller;
   }

   @Override
   public MarshallableEntry create(ByteBuffer key, ByteBuffer valueBytes) {
      return create(key, valueBytes, (ByteBuffer) null, null, -1, -1);
   }

   @Override
   public MarshallableEntry create(ByteBuffer key, ByteBuffer valueBytes, ByteBuffer metadataBytes, ByteBuffer internalMetadataBytes, long created, long lastUsed) {
      return new MarshallableEntryImpl<>(key, valueBytes, metadataBytes, internalMetadataBytes, created, lastUsed, marshaller);
   }

   @Override
   public MarshallableEntry create(Object key, ByteBuffer valueBytes, ByteBuffer metadataBytes,
         ByteBuffer internalMetadataBytes, long created, long lastUsed) {
      return new MarshallableEntryImpl<>(key, valueBytes, metadataBytes, internalMetadataBytes, created, lastUsed,
            marshaller);
   }

   @Override
   public MarshallableEntry create(Object key) {
      return create(key, MarshalledValueImpl.EMPTY);
   }

   @Override
   public MarshallableEntry create(Object key, Object value) {
      return create(key, value, null, null, -1, -1);
   }

   @Override
   public MarshallableEntry create(Object key, Object value, Metadata metadata, PrivateMetadata internalMetadata,
         long created, long lastUsed) {
      PrivateMetadata privateMetadataToUse = internalMetadata != null && !internalMetadata.isEmpty() ? internalMetadata : null;
      if (metadata == null || metadata.isEmpty()) {
         return new MarshallableEntryImpl<>(key, value, null, privateMetadataToUse, -1, -1, marshaller);
      }
      return new MarshallableEntryImpl<>(key, value, metadata, privateMetadataToUse, created, lastUsed, marshaller);
   }

   @Override
   public MarshallableEntry create(Object key, MarshalledValue value) {
      return new MarshallableEntryImpl<>(key, value.getValueBytes(), value.getMetadataBytes(), value.getInternalMetadataBytes(), value.getCreated(), value.getLastUsed(), marshaller);
   }

   @Override
   public MarshallableEntry cloneWithExpiration(MarshallableEntry me, long creationTime, long lifespan) {
      Metadata metadata = me.getMetadata();
      Metadata.Builder builder;
      if (metadata != null) {
         // Lifespan already applied, nothing to add here
         if (metadata.lifespan() > 0) {
            return me;
         }
         builder = metadata.builder();
      } else {
         builder = new EmbeddedMetadata.Builder();
      }
      metadata = builder.lifespan(lifespan).build();
      if (!(me instanceof MarshallableEntryImpl)) {
         return new MarshallableEntryImpl(me.getKey(), me.getValue(), metadata, me.getInternalMetadata(), creationTime, creationTime, marshaller);
      }
      MarshallableEntryImpl meCast = (MarshallableEntryImpl) me;
      meCast.metadata = metadata;
      meCast.metadataBytes = null;
      meCast.created = creationTime;
      return meCast;
   }

   @Override
   public MarshallableEntry getEmpty() {
      return EMPTY;
   }
}
