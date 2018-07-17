package org.infinispan.manager.impl;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commons.marshall.UserObjectInput;
import org.infinispan.marshall.core.MarshalledEntryFactory;
import org.infinispan.commons.marshall.UserObjectOutput;
import org.infinispan.util.concurrent.CompletableFutures;

/**
 * Replicable Command that runs the given Runnable
 *
 * @author wburns
 * @since 8.2
 */
public class ReplicableCommandRunnable implements ReplicableCommand {

   public static final byte COMMAND_ID = 59;

   private Runnable runnable;

   public ReplicableCommandRunnable() {

   }

   public ReplicableCommandRunnable(Runnable runnable) {
      this.runnable = runnable;
   }

   @Override
   public CompletableFuture<Object> invokeAsync() throws Throwable {
      runnable.run();
      return CompletableFutures.completedNull();
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void readFrom(UserObjectInput input) throws IOException, ClassNotFoundException {
      runnable = (Runnable) input.readObject();
   }

   @Override
   public void writeTo(UserObjectOutput output, MarshalledEntryFactory entryFactory) throws IOException {
      output.writeObject(runnable);
   }

   @Override
   public boolean isReturnValueExpected() {
      return false;
   }

   @Override
   public boolean canBlock() {
      return true;
   }
}
