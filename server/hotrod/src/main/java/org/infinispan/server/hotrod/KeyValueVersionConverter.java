package org.infinispan.server.hotrod;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.container.versioning.NumericVersion;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverter;
import org.infinispan.notifications.cachelistener.filter.EventType;

class KeyValueVersionConverter implements CacheEventConverter<byte[], byte[], byte[]> {
   private KeyValueVersionConverter() {
   }

   public static KeyValueVersionConverter SINGLETON = new KeyValueVersionConverter();

   @Override
   public byte[] convert(byte[] key, byte[] oldValue, Metadata oldMetadata, byte[] newValue, Metadata newMetadata, EventType eventType) {
      int capacity = UnsignedNumeric.sizeUnsignedInt(key.length) + key.length +
            (newValue != null ? UnsignedNumeric.sizeUnsignedInt(newValue.length) + newValue.length + 8 : 0);

      byte[] out = new byte[capacity];
      int offset = UnsignedNumeric.writeUnsignedInt(out, 0, key.length);
      offset += putBytes(key, offset, out);
      if (newValue != null) {
         offset += UnsignedNumeric.writeUnsignedInt(out, offset, newValue.length);
         offset += putBytes(newValue, offset, out);
         putLong(((NumericVersion) newMetadata.version()).getVersion(), offset, out);
      }
      return out;
   }

   private int putBytes(byte[] bytes, int offset, byte[] out) {
      int localOffset = offset;
      for (byte b : bytes) {
         out[localOffset] = b;
         localOffset += 1;
      }
      return localOffset - offset;
   }

   private int putLong(long l, int offset, byte[] out) {
      out[offset] = (byte) (l >> 56);
      out[offset + 1] = (byte) (l >> 48);
      out[offset + 2] = (byte) (l >> 40);
      out[offset + 3] = (byte) (l >> 32);
      out[offset + 4] = (byte) (l >> 24);
      out[offset + 5] = (byte) (l >> 16);
      out[offset + 6] = (byte) (l >> 8);
      out[offset + 7] = (byte) l;
      return offset + 8;
   }

   @Override
   public boolean useRequestFormat() {
      return true;
   }

   static class Externalizer extends AbstractExternalizer<KeyValueVersionConverter> {
      @Override
      public Set<Class<? extends KeyValueVersionConverter>> getTypeClasses() {
         return Collections.singleton(KeyValueVersionConverter.class);
      }

      @Override
      public void writeObject(ObjectOutput output, KeyValueVersionConverter object) throws IOException {

      }

      @Override
      public KeyValueVersionConverter readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return KeyValueVersionConverter.SINGLETON;
      }
   }
}
