package org.infinispan.manager.impl;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commons.marshall.UserObjectInput;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.core.MarshalledEntryFactory;
import org.infinispan.commons.marshall.UserObjectOutput;

/**
 * Replicable Command that runs the given Function passing the {@link EmbeddedCacheManager} as an argument
 *
 * @author wburns
 * @since 8.2
 */
public class ReplicableCommandManagerFunction implements ReplicableCommand {

   public static final byte COMMAND_ID = 60;

   private Function<? super EmbeddedCacheManager, ?> function;
   @Inject private EmbeddedCacheManager manager;

   public ReplicableCommandManagerFunction() {

   }

   public ReplicableCommandManagerFunction(Function<? super EmbeddedCacheManager, ?> function) {
      this.function = function;
   }

   @Override
   public CompletableFuture<Object> invokeAsync() throws Throwable {
      return CompletableFuture.completedFuture(function.apply(manager));
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void readFrom(UserObjectInput input) throws IOException, ClassNotFoundException {
      function = (Function<? super EmbeddedCacheManager, ?>) input.readObject();
   }

   @Override
   public void writeTo(UserObjectOutput output, MarshalledEntryFactory entryFactory) throws IOException {
      output.writeObject(function);
   }

   @Override
   public boolean isReturnValueExpected() {
      return true;
   }

   @Override
   public boolean canBlock() {
      return true;
   }
}
