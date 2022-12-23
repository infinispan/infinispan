package org.infinispan.remoting.inboundhandler;

import java.time.Duration;

/**
 *
 *
 * since 15.0
 */
public interface BlockHandler {

   boolean isBlocked();

   void awaitUntilBlocked(Duration timeout) throws InterruptedException;

   void unblock();

   void awaitUntilCommandCompleted(Duration duration) throws InterruptedException;

}
