package org.infinispan.commands.irac;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.metadata.Metadata;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.xsite.BackupReceiver;
import org.infinispan.xsite.XSiteReplicateCommand;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public class IracUpdateKeyCommand extends XSiteReplicateCommand {

   public static final byte COMMAND_ID = 83;

   private static final byte CLEAR_OP = 0;
   private static final byte REMOVE_OP = 1;
   private static final byte PUT_OP = 2;

   private static Operation CLEAR_OPERATION = new Operation() {
      @Override
      public byte getType() {
         return CLEAR_OP;
      }

      @Override
      public CompletionStage<Void> execute(BackupReceiver receiver) {
         return receiver.clearKeys();
      }

      @Override
      public String toString() {
         return "CLEAR()";
      }
   };

   private Operation operation;

   @SuppressWarnings("unused")
   public IracUpdateKeyCommand() {
      super(null);
   }

   public IracUpdateKeyCommand(ByteString cacheName) {
      super(cacheName);
   }

   public IracUpdateKeyCommand(ByteString cacheName, Object key, Object value, Metadata metadata) {
      super(cacheName);
      if (key == null) {
         this.operation = CLEAR_OPERATION;
      } else if (value == null) {
         RemoveOperation op = new RemoveOperation();
         op.key = key;
         this.operation = op;
      } else {
         PutOperation op = new PutOperation();
         op.key = key;
         op.value = value;
         op.metadata = metadata;
         this.operation = op;
      }
   }

   private static Operation createOperation(byte type) {
      switch (type) {
         case CLEAR_OP:
            return CLEAR_OPERATION;
         case PUT_OP:
            return new PutOperation();
         case REMOVE_OP:
            return new RemoveOperation();
         default:
            throw new IllegalStateException("Unknown type: " + type);
      }
   }

   @Override
   public CompletionStage<Void> performInLocalSite(BackupReceiver receiver, boolean preserveOrder) {
      assert !preserveOrder : "IRAC Update Command sent asynchronously!";
      return operation.execute(receiver);
   }

   @Override
   public CompletableFuture<Object> invokeAsync() throws Throwable {
      //no-op
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
      output.writeByte(operation.getType());
      operation.writeTo(output);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      this.operation = createOperation(input.readByte());
      this.operation.readFrom(input);
   }

   @Override
   public String toString() {
      return "IracUpdateKeyCommand{" +
             "operation=" + operation +
             '}';
   }

   private interface Operation {
      byte getType();

      CompletionStage<Void> execute(BackupReceiver receiver);

      default void writeTo(ObjectOutput out) throws IOException {
      }

      default void readFrom(ObjectInput in) throws IOException, ClassNotFoundException {
      }
   }

   private static class RemoveOperation implements Operation {

      private Object key;

      @Override
      public byte getType() {
         return REMOVE_OP;
      }

      @Override
      public CompletionStage<Void> execute(BackupReceiver receiver) {
         return receiver.removeKey(key);
      }

      @Override
      public void writeTo(ObjectOutput out) throws IOException {
         out.writeObject(key);
      }

      @Override
      public void readFrom(ObjectInput in) throws IOException, ClassNotFoundException {
         this.key = in.readObject();
      }

      @Override
      public String toString() {
         return "REMOVE(" + key + ")";
      }
   }

   private static class PutOperation implements Operation {
      private Object key;
      private Object value;
      private Metadata metadata;

      @Override
      public byte getType() {
         return PUT_OP;
      }

      @Override
      public CompletionStage<Void> execute(BackupReceiver receiver) {
         return receiver.putKeyValue(key, value, metadata);
      }

      @Override
      public void writeTo(ObjectOutput out) throws IOException {
         out.writeObject(key);
         out.writeObject(value);
         out.writeObject(metadata);
      }

      @Override
      public void readFrom(ObjectInput in) throws IOException, ClassNotFoundException {
         this.key = in.readObject();
         this.value = in.readObject();
         this.metadata = (Metadata) in.readObject();
      }

      @Override
      public String toString() {
         return "PUT(" + key + ", " + value + ")";
      }
   }
}
