package org.infinispan.server.hotrod;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilterConverter;
import org.infinispan.notifications.cachelistener.filter.EventType;

/**
 * A Filter and converter which filters out created events and just returns keys to save on sent. Used by near caches.
 *
 * @since 9.4
 */
public class KeyOnlyFilterConverter implements CacheEventFilterConverter<byte[], byte[], byte[]> {
   private KeyOnlyFilterConverter() {
   }

   public static KeyOnlyFilterConverter SINGLETON = new KeyOnlyFilterConverter();

   @Override
   public byte[] filterAndConvert(byte[] key, byte[] oldValue, Metadata oldMetadata, byte[] newValue, Metadata newMetadata, EventType eventType) {
      int capacity = UnsignedNumeric.sizeUnsignedInt(key.length) + key.length;
      byte[] out = new byte[capacity];
      int offset = UnsignedNumeric.writeUnsignedInt(out, 0, key.length);
      System.arraycopy(key, 0, out, offset, key.length);
      return out;
   }

   @Override
   public byte[] convert(byte[] key, byte[] oldValue, Metadata oldMetadata, byte[] newValue, Metadata newMetadata, EventType eventType) {
      return filterAndConvert(key, oldValue, oldMetadata, newValue, newMetadata, eventType);
   }

   @Override
   public boolean accept(byte[] key, byte[] oldValue, Metadata oldMetadata, byte[] newValue, Metadata newMetadata, EventType eventType) {
      return !eventType.isCreate();
   }

   static class Externalizer extends AbstractExternalizer<KeyOnlyFilterConverter> {
      @Override
      public Set<Class<? extends KeyOnlyFilterConverter>> getTypeClasses() {
         return Collections.singleton(KeyOnlyFilterConverter.class);
      }

      @Override
      public void writeObject(ObjectOutput output, KeyOnlyFilterConverter object) throws IOException {

      }

      @Override
      public KeyOnlyFilterConverter readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return KeyOnlyFilterConverter.SINGLETON;
      }
   }
}
