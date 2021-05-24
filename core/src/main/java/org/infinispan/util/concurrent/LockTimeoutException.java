package org.infinispan.util.concurrent;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.Optional;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.remoting.RemoteException;

/**
 * Thrown when lock acquisition fails due to timeout.
 *
 * @author Pedro Ruivo
 * @since 13.0
 */
public class LockTimeoutException extends TimeoutException implements Serializable {

   private static final byte GENERIC = 0;
   private static final byte NULL = 1;
   private static final byte COMMAND_ID = 3;

   private static final long serialVersionUID = 4312558309429074260L;
   private final Object lockOwner;

   public LockTimeoutException(String msg, Object lockOwner) {
      this(msg, lockOwner, true);
   }

   private LockTimeoutException(String msg, Object lockOwner, boolean stackTrace) {
      super(msg, null, true, stackTrace);
      this.lockOwner = lockOwner;
   }

   public Object getLockOwner() {
      return lockOwner;
   }

   public static Optional<LockTimeoutException> findLockTimeoutException(Throwable throwable) {
      if (throwable instanceof LockTimeoutException) {
         return Optional.of((LockTimeoutException) throwable);
      }
      if (throwable instanceof RemoteException) {
         return findLockTimeoutException(throwable.getCause());
      }
      return Optional.empty();
   }

   public static void writeTo(ObjectOutput out, LockTimeoutException exception) throws IOException {
      //skip getCause() & stack since it doesn't contain anything useful
      //the exception is created by the timeout-executor
      out.writeUTF(exception.getMessage());
      Object owner = exception.getLockOwner();
      if (owner == null) {
         out.writeByte(NULL);
      } else if (owner instanceof CommandInvocationId) {
         out.writeByte(COMMAND_ID);
         CommandInvocationId.writeTo(out, (CommandInvocationId) owner);
      } else {
         out.writeByte(GENERIC);
         out.writeObject(owner);
      }
   }

   public static LockTimeoutException readFrom(ObjectInput in) throws IOException, ClassNotFoundException {
      //stack & cause not send.
      //don't collect "local" stack trace since it isn't needed (nothing useful in there)
      String msg = in.readUTF();
      byte type = in.readByte();
      switch (type) {
         case NULL:
            return new LockTimeoutException(msg, null, false);
         case GENERIC:
            return new LockTimeoutException(msg, in.readObject(), false);
         case COMMAND_ID:
            return new LockTimeoutException(msg, CommandInvocationId.readFrom(in), false);
         default:
            throw new IllegalStateException("Unexpected LockTimeoutException type: " + type);
      }
   }
}
