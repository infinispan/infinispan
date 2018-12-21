package org.infinispan.marshall.persistence.impl;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.util.Util;
import org.infinispan.marshall.core.Ids;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.metadata.InternalMetadata;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.InternalMetadataImpl;

/**
 * @author Mircea Markus
 * @since 6.0
 */
@Deprecated
public class MarshalledEntryImpl<K,V> extends MarshallableEntryImpl<K,V> implements MarshalledEntry<K,V> {

   MarshalledEntryImpl(ByteBuffer keyBytes, ByteBuffer valueBytes, ByteBuffer metadataBytes, Marshaller marshaller) {
      InternalMetadata internalMetadata = metadataBytes == null ? null: unmarshall(metadataBytes, marshaller);
      this.keyBytes = keyBytes;
      this.valueBytes = valueBytes;
      this.metadataBytes = metadataBytes;
      this.created = created(internalMetadata);
      this.lastUsed = lastUsed(internalMetadata);
      this.marshaller = marshaller;
   }

   MarshalledEntryImpl(K key, ByteBuffer valueBytes, ByteBuffer metadataBytes, Marshaller marshaller) {
      this(marshall(key, marshaller), valueBytes, metadataBytes, marshaller);
      this.key = key;
   }

   MarshalledEntryImpl(K key, V value, InternalMetadata metadata, Marshaller marshaller) {
      super(key, value, InternalMetadataImpl.extractMetadata(metadata), created(metadata), lastUsed(metadata), marshaller);
   }

   private static long created(InternalMetadata internalMetadata) {
      return internalMetadata == null ? -1 : internalMetadata.created();
   }

   private static long lastUsed(InternalMetadata internalMetadata) {
      return internalMetadata == null ? -1 : internalMetadata.lastUsed();
   }

   @Override
   public InternalMetadata getMetadata() {
      Metadata metadata = super.getMetadata();
      if (metadata == null)
         return null;

      return metadata instanceof InternalMetadata ? (InternalMetadata) metadata : new InternalMetadataImpl(metadata, created(), lastUsed());
   }

   public static class Externalizer extends AbstractExternalizer<MarshalledEntryImpl> {

      private static final long serialVersionUID = -5291318076267612501L;

      private final Marshaller marshaller;

      public Externalizer(Marshaller marshaller) {
         this.marshaller = marshaller;
      }

      @Override
      public void writeObject(ObjectOutput output, MarshalledEntryImpl me) throws IOException {
         output.writeObject(me.getKeyBytes());
         output.writeObject(me.getValueBytes());
         output.writeObject(me.getMetadataBytes());
      }

      @Override
      public MarshalledEntryImpl readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            ByteBuffer keyBytes = (ByteBuffer) input.readObject();
            ByteBuffer valueBytes = (ByteBuffer) input.readObject();
            ByteBuffer metadataBytes = (ByteBuffer) input.readObject();
            return new MarshalledEntryImpl(keyBytes, valueBytes, metadataBytes, marshaller);
      }

      @Override
      public Integer getId() {
         return Ids.MARSHALLED_ENTRY_ID;
      }

      @Override
      @SuppressWarnings("unchecked")
      public Set<Class<? extends MarshalledEntryImpl>> getTypeClasses() {
         return Util.asSet(MarshalledEntryImpl.class);
      }
   }
}
