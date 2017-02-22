package org.infinispan.commands.write;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.CommandAckCollector;

/**
 * A command that represents an acknowledge sent by the primary owner to the originator.
 * <p>
 * The acknowledge signals a successful or unsuccessful execution of the operation and it contains the return value of
 * the operation.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public class PrimaryAckCommand extends BaseRpcCommand {

   public static final byte COMMAND_ID = 30;
   private static final byte SUCCESS = 1;
   private static final byte BOOLEAN = 1 << 1;
   private static final byte NO_RETURN = 1 << 2;
   private byte type;
   private CommandInvocationId commandInvocationId;
   private Object returnValue;
   private CommandAckCollector commandAckCollector;
   private int topologyId;

   public PrimaryAckCommand() {
      super(null);
   }

   public PrimaryAckCommand(ByteString cacheName) {
      super(cacheName);
   }

   public void initCommandInvocationIdAndTopologyId(CommandInvocationId id, int topologyId) {
      this.commandInvocationId = id;
      this.topologyId = topologyId;
   }

   public void ack() {
      commandAckCollector.primaryAck(commandInvocationId, returnValue, isSet(SUCCESS), getOrigin(), topologyId);

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
      output.writeInt(topologyId);
      CommandInvocationId.writeTo(output, commandInvocationId);
      output.writeByte(type);
      if (!isSet(NO_RETURN) && !isSet(BOOLEAN)) {
         output.writeObject(returnValue);
      }
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      topologyId = input.readInt();
      commandInvocationId = CommandInvocationId.readFrom(input);
      type = input.readByte();
      if (!isSet(NO_RETURN)) {
         returnValue = isSet(BOOLEAN) ? isSet(SUCCESS) : input.readObject();
      }
   }

   public void initWithReturnValue(boolean success, Object returnValue) {
      this.returnValue = returnValue;
      if (success) {
         set(SUCCESS);
      }
   }

   public void initWithBoolReturnValue(boolean success) {
      this.returnValue = success;
      set(BOOLEAN);
      if (success) {
         set(SUCCESS);
      }
   }

   public void initWithoutReturnValue(boolean success) {
      set(NO_RETURN);
      if (success) {
         set(SUCCESS);
      }
   }

   public void setCommandAckCollector(CommandAckCollector commandAckCollector) {
      this.commandAckCollector = commandAckCollector;
   }

   @Override
   public String toString() {
      return "PrimaryAckCommand{" +
            "commandInvocationId=" + commandInvocationId +
            ", returnValue=" + returnValue +
            ", type=" + type +
            ", topologyId=" + topologyId +
            '}';
   }

   private void set(byte anotherBitSet) {
      this.type |= anotherBitSet;
   }

   private boolean isSet(byte anotherBitSet) {
      return (type & anotherBitSet) == anotherBitSet;
   }
}
