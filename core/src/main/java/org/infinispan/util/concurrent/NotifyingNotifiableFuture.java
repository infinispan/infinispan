package org.infinispan.util.concurrent;

import java.util.concurrent.Future;

/**
 * An internal interface which adds the ability to inform the future of completion.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public interface NotifyingNotifiableFuture<Object> extends NotifyingFuture<Object> {
   void notifyDone();

   void setNetworkFuture(Future<java.lang.Object> future);
}
