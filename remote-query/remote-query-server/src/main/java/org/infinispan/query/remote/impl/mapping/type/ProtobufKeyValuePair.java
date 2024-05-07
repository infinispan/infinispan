package org.infinispan.query.remote.impl.mapping.type;

public class ProtobufKeyValuePair {

   private final byte[] key;
   private final byte[] value;

   public ProtobufKeyValuePair(byte[] key, byte[] value) {
      this.key = key;
      this.value = value;
   }

   public byte[] key() {
      return key;
   }
   public byte[] value() {
      return value;
   }
}
