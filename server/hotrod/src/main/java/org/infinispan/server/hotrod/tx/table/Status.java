package org.infinispan.server.hotrod.tx.table;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * //TODO document this!
 *
 * @author Pedro Ruivo
 * @since 9.4
 */
public enum Status {
   //used as return values
   OK(0),
   ERROR(1),
   NO_TRANSACTION(2),

   //real status
   ACTIVE(3),
   PREPARING(4),
   PREPARED(5),
   MARK_COMMIT(6),
   COMMITTED(7),
   MARK_ROLLBACK(8),
   ROLLED_BACK(9);

   private static final Status[] CACHE;

   static {
      Status[] values = Status.values();
      CACHE = new Status[values.length];
      for (Status s : values) {
         CACHE[s.value] = s;
      }
   }

   public final byte value;

   Status(int value) {
      this.value = (byte) value;
   }

   public static void writeTo(ObjectOutput output, Status status) throws IOException {
      output.writeByte(status.value);
   }

   public static Status readFrom(ObjectInput input) throws IOException {
      return valueOf(input.readByte());
   }

   public static Status valueOf(byte b) {
      return CACHE[b];
   }
}
