package org.infinispan.manager.impl;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.manager.EmbeddedCacheManager;

/**
 * Replicable Command that runs the given Function passing the {@link EmbeddedCacheManager} as an argument
 *
 * @author wburns
 * @since 8.2
 */
public class ReplicableCommandManagerFunction implements ReplicableCommand {

   public static final byte COMMAND_ID = 60;

   private Function<? super EmbeddedCacheManager, ?> function;
   private EmbeddedCacheManager manager;

   public ReplicableCommandManagerFunction() {

   }

   public ReplicableCommandManagerFunction(Function<? super EmbeddedCacheManager, ?> function) {
      this.function = function;
   }

   @Inject
   public void inject(EmbeddedCacheManager manager) {
      this.manager = manager;
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
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      function = (Function<? super EmbeddedCacheManager, ?>) input.readObject();
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
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
