package org.infinispan.manager.impl;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import javax.security.auth.Subject;

import org.infinispan.commands.GlobalRpcCommand;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.security.Security;
import org.infinispan.util.concurrent.BlockingManager;

/**
 * Replicable Command that runs the given Function passing the {@link EmbeddedCacheManager} as an argument
 *
 * @author wburns
 * @since 8.2
 */
@Scope(Scopes.NONE)
public class ReplicableManagerFunctionCommand implements GlobalRpcCommand {

   public static final byte COMMAND_ID = 60;

   private Function<? super EmbeddedCacheManager, ?> function;
   private Subject subject;

   public ReplicableManagerFunctionCommand() {

   }

   public ReplicableManagerFunctionCommand(Function<? super EmbeddedCacheManager, ?> function, Subject subject) {
      this.function = function;
      this.subject = subject;
   }

   @Override
   public CompletableFuture<Object> invokeAsync(GlobalComponentRegistry globalComponentRegistry) throws Throwable {
      BlockingManager bm = globalComponentRegistry.getComponent(BlockingManager.class);
      return bm.supplyBlocking(() -> {
         if (subject == null) {
            return function.apply(new UnwrappingEmbeddedCacheManager(globalComponentRegistry.getCacheManager()));
         } else {
            return Security.doAs(subject, function, new UnwrappingEmbeddedCacheManager(globalComponentRegistry.getCacheManager()));
         }
      }, "replicable-manager-function").toCompletableFuture();
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      function = (Function<? super EmbeddedCacheManager, ?>) input.readObject();
      subject = (Subject) input.readObject();
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      output.writeObject(function);
      output.writeObject(subject);
   }

   @Override
   public boolean isReturnValueExpected() {
      return true;
   }

   @Override
   public boolean canBlock() {
      // Note that it is highly possible that a user command could block, and some internal Infinispan ones already do
      // This should be remedied with https://issues.redhat.com/browse/ISPN-11482
      return false;
   }
}
