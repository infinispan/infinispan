package org.infinispan.server;

import java.util.concurrent.CompletableFuture;

/**
 * ExitHandler provides alternate implementations for {@link System#exit(int)} to use in different scenarios (e.g.
 * tests)
 *
 * @author Tristan Tarrant
 * @since 10.0
 */

public abstract class ExitHandler {
   protected final CompletableFuture<Integer> exitFuture;

   ExitHandler() {
      exitFuture = new CompletableFuture<>();
   }

   public CompletableFuture<Integer> getExitFuture() {
      return exitFuture;
   }

   public abstract void exit(int exitCode);
}
