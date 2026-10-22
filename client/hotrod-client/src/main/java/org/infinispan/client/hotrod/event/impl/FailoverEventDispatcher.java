package org.infinispan.client.hotrod.event.impl;

import java.util.concurrent.CompletableFuture;

/** A listener whose registration can move to another server after a connection loss. */
interface FailoverEventDispatcher {
   CompletableFuture<Void> executeFailover();

   default void invokeFailoverEvent() {
   }
}
