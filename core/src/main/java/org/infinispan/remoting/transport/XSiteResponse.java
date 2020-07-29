package org.infinispan.remoting.transport;

import java.util.concurrent.CompletionStage;

import org.infinispan.xsite.XSiteBackup;

/**
 * An extension to {@link CompletionStage} with {@link #whenCompleted(XSiteResponseCompleted)}.
 * <p>
 * It provides a method to register the cross-site request statistics and data for the {@link
 * org.infinispan.xsite.OfflineStatus}.
 * <p>
 * Note: do not complete the {@link java.util.concurrent.CompletableFuture} returned by {@link #toCompletableFuture()}.
 *
 * @author Pedro Ruivo
 * @since 10.0
 */
public interface XSiteResponse<O> extends CompletionStage<O> {

   void whenCompleted(XSiteResponseCompleted xSiteResponseCompleted);

   @FunctionalInterface
   interface XSiteResponseCompleted {
      void onCompleted(XSiteBackup backup, long sendTimeNanos, long durationNanos, Throwable throwable);
   }

}
