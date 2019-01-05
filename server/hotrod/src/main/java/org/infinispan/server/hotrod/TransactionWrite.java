package org.infinispan.server.hotrod;

import org.infinispan.commons.util.Util;
import org.infinispan.metadata.Metadata;
import org.infinispan.server.hotrod.tx.ControlByte;

class TransactionWrite {
   final byte[] key;
   final long versionRead;
   final Metadata.Builder metadata;
   final byte[] value;
   private final byte control;

   TransactionWrite(byte[] key, long versionRead, byte control, byte[] value, Metadata.Builder metadata) {
      this.key = key;
      this.versionRead = versionRead;
      this.control = control;
      this.value = value;
      this.metadata = metadata;
   }

   public boolean isRemove() {
      return ControlByte.REMOVE_OP.hasFlag(control);
   }

   @Override
   public String toString() {
      return "TransactionWrite{" +
            "key=" + Util.printArray(key, true) +
            ", versionRead=" + versionRead +
            ", control=" + ControlByte.prettyPrint(control) +
            ", metadata=" + metadata +
            ", value=" + Util.printArray(value, true) +
            '}';
   }

   boolean skipRead() {
      return ControlByte.NOT_READ.hasFlag(control);
   }

   boolean wasNonExisting() {
      return ControlByte.NON_EXISTING.hasFlag(control);
   }
}
