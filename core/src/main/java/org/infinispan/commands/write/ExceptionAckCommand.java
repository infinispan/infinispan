package org.infinispan.commands.write;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.infinispan.commands.ReplicableCommand;

/**
 * A command that represents an exception acknowledge sent by any owner.
 * <p>
 * The acknowledge represents an unsuccessful execution of the operation.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public class ExceptionAckCommand implements ReplicableCommand {

   public static final byte COMMAND_ID = 42;
   private Throwable throwable;
   private long id;
   private int topologyId;

   public ExceptionAckCommand() {
   }


   public ExceptionAckCommand(long id, Throwable throwable, int topologyId) {
      this.id = id;
      this.throwable = throwable;
      this.topologyId = topologyId;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public boolean isReturnValueExpected() {
      return false;
   }

   @Override
   public boolean canBlock() {
      return false;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      output.writeLong(id);
      output.writeObject(throwable);
      output.writeInt(topologyId);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      id = input.readLong();
      throwable = (Throwable) input.readObject();
      topologyId = input.readInt();
   }

   public Throwable getThrowable() {
      return throwable;
   }

   public long getId() {
      return id;
   }

   public int getTopologyId() {
      return topologyId;
   }

   @Override
   public String toString() {
      return "ExceptionAckCommand{" +
            "id=" + id +
            ", throwable=" + throwable +
            ", topologyId=" + topologyId +
            '}';
   }
}
