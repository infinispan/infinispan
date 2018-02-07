package org.infinispan.remoting.transport.impl;

import org.infinispan.remoting.responses.ValidResponse;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.ResponseCollectors;
import org.infinispan.remoting.transport.ValidResponseCollector;
import org.infinispan.util.concurrent.CompletableFutures;

/**
 * Response collector that discards successful responses and returns {@code null}.
 *
 * <p>Throws an exception if it receives at least one exception response, or if
 * a node is suspected and {@code ignoreLeavers == true}.
 *
 * @author Dan Berindei
 * @since 9.2
 */
public class VoidResponseCollector extends ValidResponseCollector<Void> {

   //note: can't be a singleton since it has state (exception field)
   private final boolean ignoreLeavers;
   private Exception exception;

   public static VoidResponseCollector validOnly() {
      return new VoidResponseCollector(false);
   }

   public static VoidResponseCollector ignoreLeavers() {
      return new VoidResponseCollector(true);
   }

   private VoidResponseCollector(boolean ignoreLeavers) {
      this.ignoreLeavers = ignoreLeavers;
   }

   @Override
   protected Void addTargetNotFound(Address sender) {
      if (!ignoreLeavers) {
         recordException(ResponseCollectors.remoteNodeSuspected(sender));
      }
      return null;
   }

   @Override
   protected Void addException(Address sender, Exception exception) {
      recordException(ResponseCollectors.wrapRemoteException(sender, exception));
      return null;
   }

   private void recordException(Exception e) {
      if (this.exception == null) {
         this.exception = e;
      } else {
         this.exception.addSuppressed(e);
      }
   }

   @Override
   protected Void addValidResponse(Address sender, ValidResponse response) {
      return null;
   }

   @Override
   public Void finish() {
      if (exception != null) {
         throw CompletableFutures.asCompletionException(exception);
      }
      return null;
   }
}
