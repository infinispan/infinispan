package org.infinispan.util.concurrent;

import org.infinispan.commons.util.concurrent.NotifyingFutureImpl;

/**
 * LegacyNotifyingFutureAdaptor.
 *
 * @author Tristan Tarrant
 * @since 6.0
 */
@Deprecated
public class LegacyNotifyingFutureAdaptor<T> extends NotifyingFutureImpl<T> implements NotifyingFuture<T> {

   @Override
   public NotifyingFuture<T> attachListener(org.infinispan.util.concurrent.FutureListener<T> listener) {
      throw new UnsupportedOperationException();
   }

}
