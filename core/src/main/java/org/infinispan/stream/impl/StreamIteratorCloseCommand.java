package org.infinispan.stream.impl;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.CompletableFuture;

import org.infinispan.commands.InitializableCommand;
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.CompletableFutures;

/**
 * Stream iterator command that unregisters an iterator so it doesn't consume memory unnecessarily
 */
public class StreamIteratorCloseCommand extends BaseRpcCommand implements InitializableCommand {
   public static final byte COMMAND_ID = 72;

   protected LocalStreamManager lsm;

   protected Object id;

   public Object getId() {
      return id;
   }

   // Only here for CommandIdUniquenessTest
   private StreamIteratorCloseCommand() { super(null); }

   public StreamIteratorCloseCommand(ByteString cacheName) {
      super(cacheName);
   }

   public StreamIteratorCloseCommand(ByteString cacheName, Address origin, Object id) {
      super(cacheName);
      setOrigin(origin);
      this.id = id;
   }

   @Override
   public void init(ComponentRegistry componentRegistry, boolean isRemote) {
      this.lsm = componentRegistry.getLocalStreamManager().running();
   }

   @Override
   public CompletableFuture<Object> invokeAsync() throws Throwable {
      lsm.closeIterator(getOrigin(), id);
      return CompletableFutures.completedNull();
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      output.writeObject(id);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      id = input.readObject();
   }

   @Override
   public boolean isReturnValueExpected() {
      return false;
   }

   @Override
   public boolean canBlock() {
      return false;
   }
}
