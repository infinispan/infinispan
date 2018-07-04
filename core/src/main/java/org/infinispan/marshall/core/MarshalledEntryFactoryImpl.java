package org.infinispan.marshall.core;

import static org.infinispan.factories.KnownComponentNames.USER_MARSHALLER;

import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.metadata.Metadata;

/**
 * @author Mircea Markus
 * @since 6.0
 */
public class MarshalledEntryFactoryImpl implements MarshalledEntryFactory {

   @Inject @ComponentName(USER_MARSHALLER) private Marshaller marshaller;

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
   public MarshalledEntry newMarshalledEntry(Object key, Object value, Metadata im) {
      return new MarshalledEntryImpl(key, value, im, marshaller);
   }
}
