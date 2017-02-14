package org.infinispan.commands.write;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.CompletableFuture;

import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.CommandAckCollector;
import org.infinispan.util.concurrent.CompletableFutures;

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
   private static final Type[] CACHED_TYPE = Type.values();
   private long id;
   private Object returnValue;
   private Type type;
   private CommandAckCollector commandAckCollector;
   private int topologyId;

   public PrimaryAckCommand() {
      super(null);
   }

   public PrimaryAckCommand(ByteString cacheName) {
      super(cacheName);
   }

   private static Type valueOf(int index) {
      return CACHED_TYPE[index];
   }

   @Override
   public CompletableFuture<Object> invokeAsync() throws Throwable {
      switch (type) {
         case SUCCESS_WITH_BOOL_RETURN_VALUE:
         case SUCCESS_WITH_RETURN_VALUE:
         case SUCCESS_WITHOUT_RETURN_VALUE:
            commandAckCollector.primaryAck(id, returnValue, true, getOrigin(), topologyId);
            break;
         case UNSUCCESSFUL_WITH_BOOL_RETURN_VALUE:
         case UNSUCCESSFUL_WITH_RETURN_VALUE:
         case UNSUCCESSFUL_WITHOUT_RETURN_VALUE:
            commandAckCollector.primaryAck(id, returnValue, false, getOrigin(), topologyId);
            break;
      }
      return CompletableFutures.completedNull();
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
      output.writeInt(topologyId);
      output.writeLong(id);
      MarshallUtil.marshallEnum(type, output);
      switch (type) {
         case SUCCESS_WITH_RETURN_VALUE:
         case UNSUCCESSFUL_WITH_RETURN_VALUE:
            output.writeObject(returnValue);
            break;
         default:
      }
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      topologyId = input.readInt();
      id = input.readLong();
      type = MarshallUtil.unmarshallEnum(input, PrimaryAckCommand::valueOf);
      assert type != null;
      switch (type) {
         case SUCCESS_WITH_RETURN_VALUE:
         case UNSUCCESSFUL_WITH_RETURN_VALUE:
            returnValue = input.readObject();
            break;
         case SUCCESS_WITH_BOOL_RETURN_VALUE:
            returnValue = true;
            break;
         case UNSUCCESSFUL_WITH_BOOL_RETURN_VALUE:
            returnValue = false;
            break;
         default:
      }
   }

   @Override
   public String toString() {
      return "PrimaryAckCommand{" +
            "id=" + id +
            ", returnValue=" + returnValue +
            ", type=" + type +
            ", topologyId=" + topologyId +
            '}';
   }

   public void setCommandAckCollector(CommandAckCollector commandAckCollector) {
      this.commandAckCollector = commandAckCollector;
   }

   void initCommandInvocationIdAndTopologyId(long id, int topologyId) {
      this.id = id;
      this.topologyId = topologyId;
   }

   void initWithReturnValue(boolean success, Object returnValue) {
      this.returnValue = returnValue;
      if (success) {
         type = Type.SUCCESS_WITH_RETURN_VALUE;
      } else {
         type = Type.UNSUCCESSFUL_WITH_RETURN_VALUE;
      }
   }

   void initWithBoolReturnValue(boolean success) {
      this.returnValue = success;
      if (success) {
         type = Type.SUCCESS_WITH_BOOL_RETURN_VALUE;
      } else {
         type = Type.UNSUCCESSFUL_WITH_BOOL_RETURN_VALUE;
      }
   }

   void initWithoutReturnValue(boolean success) {
      if (success) {
         type = Type.SUCCESS_WITHOUT_RETURN_VALUE;
      } else {
         type = Type.UNSUCCESSFUL_WITHOUT_RETURN_VALUE;
      }
   }

   private enum Type {
      SUCCESS_WITH_RETURN_VALUE,
      SUCCESS_WITH_BOOL_RETURN_VALUE,
      SUCCESS_WITHOUT_RETURN_VALUE,
      UNSUCCESSFUL_WITH_RETURN_VALUE,
      UNSUCCESSFUL_WITH_BOOL_RETURN_VALUE,
      UNSUCCESSFUL_WITHOUT_RETURN_VALUE
   }
}
