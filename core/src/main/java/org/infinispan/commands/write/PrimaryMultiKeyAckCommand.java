package org.infinispan.commands.write;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.CommandAckCollector;
import org.infinispan.util.concurrent.CompletableFutures;

/**
 * A command that represents an acknowledge sent by the primary owner to the originator.
 * <p>
 * The acknowledge signals a successful execution of a multi-key command, like {@link PutMapCommand}. It contains the
 * return value of this primary owner.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public class PrimaryMultiKeyAckCommand extends BaseRpcCommand {

   public static final byte COMMAND_ID = 31;
   private static final Type[] CACHED_TYPE = Type.values();
   private CommandInvocationId commandInvocationId;
   private Map<Object, Object> returnValue = Collections.emptyMap();
   private Type type;
   private CommandAckCollector commandAckCollector;
   private int topologyId;

   public PrimaryMultiKeyAckCommand() {
      super(null);
   }

   public PrimaryMultiKeyAckCommand(ByteString cacheName) {
      super(cacheName);
   }

   public PrimaryMultiKeyAckCommand(ByteString cacheName, CommandInvocationId commandInvocationId, int topologyId) {
      super(cacheName);
      this.commandInvocationId = commandInvocationId;
      this.topologyId = topologyId;
   }

   private static Type valueOf(int index) {
      return CACHED_TYPE[index];
   }

   @Override
   public CompletableFuture<Object> invokeAsync() throws Throwable {
      commandAckCollector.multiKeyPrimaryAck(commandInvocationId, getOrigin(), returnValue, topologyId);
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
   public void writeTo(ObjectOutput output) throws IOException {
      CommandInvocationId.writeTo(output, commandInvocationId);
      MarshallUtil.marshallEnum(type, output);
      output.writeInt(topologyId);
      if (type == Type.SUCCESS_WITH_RETURN_VALUE) {
         MarshallUtil.marshallMap(returnValue, output);
      }
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      commandInvocationId = CommandInvocationId.readFrom(input);
      type = MarshallUtil.unmarshallEnum(input, PrimaryMultiKeyAckCommand::valueOf);
      topologyId = input.readInt();
      assert type != null;
      if (type == Type.SUCCESS_WITH_RETURN_VALUE) {
         returnValue = MarshallUtil.unmarshallMap(input, HashMap::new);
      }
   }

   public void initWithReturnValue(Map<Object, Object> returnValue) {
      this.returnValue = returnValue;
      type = Type.SUCCESS_WITH_RETURN_VALUE;
   }

   public void initWithoutReturnValue() {
      type = Type.SUCCESS_WITHOUT_RETURN_VALUE;
   }

   public void setCommandAckCollector(CommandAckCollector commandAckCollector) {
      this.commandAckCollector = commandAckCollector;
   }

   @Override
   public String toString() {
      return "PrimaryMultiKeyAckCommand{" +
            "commandInvocationId=" + commandInvocationId +
            ", returnValue=" + returnValue +
            ", type=" + type +
            ", topologyId=" + topologyId +
            '}';
   }

   private enum Type {
      SUCCESS_WITH_RETURN_VALUE,
      SUCCESS_WITHOUT_RETURN_VALUE
   }
}
