package org.infinispan.server.hotrod.tx.table;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import javax.transaction.TransactionManager;

import org.infinispan.server.hotrod.tx.table.functions.TxFunction;

/**
 * Internal server status for the client's transactions.
 * <p>
 * The first 3 values are used as return value for the {@link TxFunction}.
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
   /**
    * The client transaction is being replayed.
    */
   ACTIVE(3),
   /**
    * The client transaction was replayed successful (versions read are validated) and the transactions is preparing.
    */
   PREPARING(4),
   /**
    * The transaction is successful prepared and it waits for the client {@link TransactionManager} decision to commit
    * or rollback
    */
   PREPARED(5),
   /**
    * The client {@link TransactionManager} decided to commit. It can't rollback anymore.
    */
   MARK_COMMIT(6),
   /**
    * The client transaction is committed. No other work can be done.
    */
   COMMITTED(7),
   /**
    * The client {@link TransactionManager} decided to rollback of we fail to replay/prepare the transaction. It can't
    * commit anymore.
    */
   MARK_ROLLBACK(8),
   /**
    * The client transaction is rolled back. No other work can be done.
    */
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
