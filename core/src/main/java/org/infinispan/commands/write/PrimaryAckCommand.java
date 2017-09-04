package org.infinispan.commands.write;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;

import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.commons.util.Util;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.CommandAckCollector;

/**
 * Used in scattered cache with biased reads. Carries both the return value of the command
 * and list of nodes that need to confirm local bias revocation before the originator can
 * finish the operation.
 */
public class PrimaryAckCommand extends BaseRpcCommand {
   public static final int COMMAND_ID = 73;

   private transient CommandAckCollector commandAckCollector;

   private long id;
   private boolean success;
   private Object value;
   private Address[] waitFor;

   public PrimaryAckCommand() {
      super(null);
   }

   public PrimaryAckCommand(ByteString cacheName) {
      super(cacheName);
   }

   public PrimaryAckCommand(ByteString cacheName, long id, boolean success, Object value, Address[] waitFor) {
      super(cacheName);
      this.id = id;
      this.success = success;
      this.value = value;
      this.waitFor = waitFor;
   }

   public void setCommandAckCollector(CommandAckCollector commandAckCollector) {
      this.commandAckCollector = commandAckCollector;
   }

   public void ack() {
      commandAckCollector.primaryAck(id, getOrigin(), value, success, waitFor);
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
   public void writeTo(ObjectOutput output) throws IOException {
      output.writeLong(id);
      output.writeBoolean(success);
      output.writeObject(value);
      if (success) {
         MarshallUtil.marshallArray(waitFor, output);
      }
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      id = input.readLong();
      success = input.readBoolean();
      value = input.readObject();
      if (success) {
         waitFor = MarshallUtil.unmarshallArray(input, Address[]::new);
      }
   }

   @Override
   public String toString() {
      final StringBuilder sb = new StringBuilder("PrimaryAckCommand{");
      sb.append("id=").append(id);
      sb.append(", success=").append(success);
      sb.append(", value=").append(Util.toStr(value));
      sb.append(", waitFor=").append(Arrays.toString(waitFor));
      sb.append('}');
      return sb.toString();
   }
}
