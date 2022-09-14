package org.infinispan.manager.impl;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import javax.security.auth.Subject;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.security.Security;

/**
 * Replicable Command that runs the given Function passing the {@link EmbeddedCacheManager} as an argument
 *
 * @author wburns
 * @since 8.2
 */
public class ReplicableCommandManagerFunction implements ReplicableCommand {

   public static final byte COMMAND_ID = 60;

   private Function<? super EmbeddedCacheManager, ?> function;
   private Subject subject;
   @Inject EmbeddedCacheManager manager;

   public ReplicableCommandManagerFunction() {

   }

   public ReplicableCommandManagerFunction(Function<? super EmbeddedCacheManager, ?> function, Subject subject) {
      this.function = function;
      this.subject = subject;
   }

   @Override
   public CompletableFuture<Object> invokeAsync() throws Throwable {
      if (subject == null) {
         return CompletableFuture.completedFuture(function.apply(manager));
      } else {
         return CompletableFuture.completedFuture(Security.doAs(subject, function, manager));
      }
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
      return true;
   }
}
