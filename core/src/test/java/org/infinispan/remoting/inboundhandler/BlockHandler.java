package org.infinispan.remoting.inboundhandler;

import java.time.Duration;

/**
 * TODO! Document
 *
 * since 15.0
 */
public interface BlockHandler {

   Duration TEN_SECONDS = Duration.ofSeconds(10);

   boolean isBlocked();

   default void awaitUntilBlocked() throws InterruptedException {
      awaitUntilBlocked(TEN_SECONDS);
   }

   void awaitUntilBlocked(Duration timeout) throws InterruptedException;

   void unblock();

   default void awaitUntilCommandCompleted() throws InterruptedException {
      awaitUntilCommandCompleted(TEN_SECONDS);
   }

   void awaitUntilCommandCompleted(Duration duration) throws InterruptedException;

}
