package org.infinispan.remoting.transport.impl;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import org.infinispan.commons.time.TimeService;
import org.infinispan.remoting.responses.ValidResponse;
import org.infinispan.remoting.transport.XSiteResponse;
import org.infinispan.xsite.XSiteBackup;

/**
 * Default implementation of {@link XSiteResponse}.
 * <p>
 * It implements {@link BiConsumer} in order to be notified when the {@link org.infinispan.remoting.transport.jgroups.SingleSiteRequest}
 * is completed.
 *
 * @author Pedro Ruivo
 * @since 10.0
 */
public class XSiteResponseImpl<O> extends CompletableFuture<O> implements XSiteResponse<O>, BiConsumer<ValidResponse, Throwable> {

   private final long sendTimeNanos;
   private final TimeService timeService;
   private final XSiteBackup xSiteBackup;
   private volatile long durationNanos;

   public XSiteResponseImpl(TimeService timeService, XSiteBackup xSiteBackup) {
      this.sendTimeNanos = timeService.time();
      this.timeService = timeService;
      this.xSiteBackup = xSiteBackup;
   }


   @Override
   public void whenCompleted(XSiteResponseCompleted xSiteResponseCompleted) {
      this.whenComplete((aVoid, throwable) -> xSiteResponseCompleted
            .onCompleted(xSiteBackup, sendTimeNanos, durationNanos, throwable));
   }

   @Override
   public void accept(ValidResponse response, Throwable throwable) {
      durationNanos = timeService.timeDuration(sendTimeNanos, TimeUnit.NANOSECONDS);
      if (throwable != null) {
         completeExceptionally(throwable);
      } else {
         //noinspection unchecked
         complete((O) response.getResponseValue());
      }
   }
}
