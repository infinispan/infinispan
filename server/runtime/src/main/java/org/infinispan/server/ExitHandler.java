package org.infinispan.server;

import java.util.concurrent.CompletableFuture;

/**
 * ExitHandler provides alternate implementations for exiting the server
 *
 * @author Tristan Tarrant
 * @since 10.0
 */

public abstract class ExitHandler {
   protected final CompletableFuture<ExitStatus> exitFuture;

   ExitHandler() {
      exitFuture = new CompletableFuture<>();
   }

   public CompletableFuture<ExitStatus> getExitFuture() {
      return exitFuture;
   }

   public abstract void exit(ExitStatus status);
}
