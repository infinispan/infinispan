package org.infinispan.commons.util.concurrent;

import java.util.concurrent.Future;

/**
 * An internal interface which adds the ability to inform the future of completion.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public interface NotifyingNotifiableFuture<T> extends NotifyingFuture<T> {
   void notifyDone();

   void setNetworkFuture(Future<T> future);
}
