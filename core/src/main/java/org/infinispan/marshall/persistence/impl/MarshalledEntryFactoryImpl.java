package org.infinispan.marshall.persistence.impl;

import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.metadata.InternalMetadata;
import org.infinispan.persistence.spi.MarshalledEntry;
import org.infinispan.persistence.spi.MarshalledEntryFactory;

/**
 * @author Mircea Markus
 * @since 6.0
 */
public class MarshalledEntryFactoryImpl implements MarshalledEntryFactory {

   private static final MarshalledEntry EMPTY = new MarshalledEntryImpl(null, null, (ByteBuffer) null, null);

   @Inject private Marshaller marshaller;

   public MarshalledEntryFactoryImpl() {
   }

   public MarshalledEntryFactoryImpl(Marshaller marshaller) {
      this.marshaller = marshaller;
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

   @Override
   public MarshalledEntry getEmpty() {
      return EMPTY;
   }
}
