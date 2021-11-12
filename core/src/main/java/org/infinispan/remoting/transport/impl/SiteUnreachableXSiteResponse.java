package org.infinispan.remoting.transport.impl;

import static org.infinispan.util.logging.Log.CLUSTER;

import java.util.concurrent.CompletableFuture;

import org.infinispan.commons.time.TimeService;
import org.infinispan.remoting.transport.SiteAddress;
import org.infinispan.remoting.transport.XSiteResponse;
import org.infinispan.remoting.transport.jgroups.SuspectException;
import org.infinispan.xsite.XSiteBackup;

/**
 * A {@link XSiteResponse} which is competed with a {@link SuspectException}.
 *
 * @since 14.0
 */
public class SiteUnreachableXSiteResponse<T> extends CompletableFuture<T> implements XSiteResponse<T> {

   private final XSiteBackup backup;
   private final long sendTimeNanos;
   private final SuspectException exception;

   public SiteUnreachableXSiteResponse(XSiteBackup backup, TimeService timeService) {
      this.backup = backup;
      this.sendTimeNanos = timeService.time();
      this.exception = CLUSTER.remoteNodeSuspected(new SiteAddress(backup.getSiteName()));
      completeExceptionally(exception);
   }

   @Override
   public void whenCompleted(XSiteResponseCompleted xSiteResponseCompleted) {
      xSiteResponseCompleted.onCompleted(backup, sendTimeNanos, -1, exception);
   }

   @Override
   public String toString() {
      return "SiteUnreachableXSiteResponse{" +
            "backup=" + backup +
            ", sendTimeNanos=" + sendTimeNanos +
            '}';
   }
}
