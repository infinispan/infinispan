package org.infinispan.server.hotrod;

import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.NumericVersion;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverter;
import org.infinispan.notifications.cachelistener.filter.EventType;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

@ProtoTypeId(ProtoStreamTypeIds.SERVER_HR_KEY_VALUE_VERSION_CONVERTER)
class KeyValueVersionConverter implements CacheEventConverter<byte[], byte[], byte[]> {

   @ProtoField(value = 1, defaultValue = "false")
   final boolean returnOldValue;

   @ProtoFactory
   static KeyValueVersionConverter protoFactory(boolean returnOldValue) {
      return returnOldValue ?
            KeyValueVersionConverter.INCLUDING_OLD_VALUE_CONVERTER :
            KeyValueVersionConverter.EXCLUDING_OLD_VALUE_CONVERTER;
   }

   private KeyValueVersionConverter(boolean returnOldValue) {
      this.returnOldValue = returnOldValue;
   }

   public static final KeyValueVersionConverter EXCLUDING_OLD_VALUE_CONVERTER = new KeyValueVersionConverter(false);
   public static final KeyValueVersionConverter INCLUDING_OLD_VALUE_CONVERTER = new KeyValueVersionConverter(true);

   @Override
   public byte[] convert(byte[] key, byte[] oldValue, Metadata oldMetadata, byte[] newValue, Metadata newMetadata, EventType eventType) {
      int capacity = UnsignedNumeric.sizeUnsignedInt(key.length) + key.length +
            (newValue != null ? UnsignedNumeric.sizeUnsignedInt(newValue.length) + newValue.length + 8 : 0);

      if (newValue == null && returnOldValue && oldValue != null) {
         capacity += UnsignedNumeric.sizeUnsignedInt(oldValue.length) + oldValue.length + 8;
      }

      byte[] out = new byte[capacity];
      int offset = UnsignedNumeric.writeUnsignedInt(out, 0, key.length);
      offset += putBytes(key, offset, out);
      if (newValue != null) {
         offset += UnsignedNumeric.writeUnsignedInt(out, offset, newValue.length);
         offset += putBytes(newValue, offset, out);
         writeVersion(newMetadata, offset, out);
      }
      if (newValue == null && returnOldValue && oldValue != null) {
         offset += UnsignedNumeric.writeUnsignedInt(out, offset, oldValue.length);
         offset += putBytes(oldValue, offset, out);
         writeVersion(oldMetadata, offset, out);
      }
      return out;
   }

   private static void writeVersion(Metadata metadata, int offset, byte[] out) {
      if (metadata == null || metadata.version() == null) {
         return;
      }
      EntryVersion version = metadata.version();
      if (version instanceof NumericVersion) {
         putLong(((NumericVersion) version).getVersion(), offset, out);
      }
   }

   private static int putBytes(byte[] bytes, int offset, byte[] out) {
      System.arraycopy(bytes, 0, out, offset, bytes.length);
      return bytes.length;
   }

   private static int putLong(long l, int offset, byte[] out) {
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
}
