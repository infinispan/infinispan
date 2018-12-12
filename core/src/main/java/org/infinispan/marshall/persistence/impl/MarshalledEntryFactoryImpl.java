package org.infinispan.marshall.persistence.impl;

import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.marshall.core.MarshalledEntryFactory;
import org.infinispan.metadata.InternalMetadata;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.MarshallableEntryFactory;

/**
 * @author Ryan Emerson
 * @since 10.0
 */
public class MarshalledEntryFactoryImpl implements MarshalledEntryFactory, MarshallableEntryFactory {

   private static final MarshallableEntry EMPTY = new MarshalledEntryImpl(null, null, (ByteBuffer) null, null);

   @Inject private Marshaller marshaller;

   public MarshalledEntryFactoryImpl() {
   }

   public MarshalledEntryFactoryImpl(Marshaller marshaller) {
      this.marshaller = marshaller;
   }

   @Override
   public MarshallableEntry create(ByteBuffer key, ByteBuffer valueBytes, ByteBuffer metadataBytes) {
      return newMarshalledEntry(key, valueBytes, metadataBytes);
   }

   @Override
   public MarshallableEntry create(Object key, ByteBuffer valueBytes, ByteBuffer metadataBytes) {
      return newMarshalledEntry(key, valueBytes, metadataBytes);
   }

   @Override
   public MarshallableEntry create(Object key, Object value, InternalMetadata im) {
      return newMarshalledEntry(key, value, im);
   }

   @Override
   public MarshallableEntry getEmpty() {
      return EMPTY;
   }

   @Override
   public MarshalledEntry newMarshalledEntry(ByteBuffer key, ByteBuffer valueBytes, ByteBuffer metadataBytes) {
      return new MarshalledEntryImpl(key, valueBytes, metadataBytes, marshaller);
   }

   @Override
   public MarshalledEntry newMarshalledEntry(Object key, ByteBuffer valueBytes, ByteBuffer metadataBytes) {
      return new MarshalledEntryImpl(key, valueBytes, metadataBytes, marshaller);
   }

   @Override
   public MarshalledEntry newMarshalledEntry(Object key, Object value, InternalMetadata im) {
      return new MarshalledEntryImpl(key, value, im, marshaller);
   }
}
