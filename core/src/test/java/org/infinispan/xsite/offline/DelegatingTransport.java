package org.infinispan.xsite.offline;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

import org.infinispan.remoting.transport.AbstractDelegatingTransport;
import org.infinispan.remoting.transport.BackupResponse;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.XSiteResponse;
import org.infinispan.util.logging.Log;
import org.infinispan.xsite.XSiteBackup;
import org.infinispan.xsite.XSiteReplicateCommand;

public class DelegatingTransport extends AbstractDelegatingTransport {

   volatile boolean fail;

   DelegatingTransport(Transport actual) {
      super(actual);
   }

   @Override
   public void start() {
      //no-op; avoid re-start the transport again...
   }

   @Override
   public BackupResponse backupRemotely(final Collection<XSiteBackup> backups, XSiteReplicateCommand rpcCommand) {
      throw new UnsupportedOperationException();
   }

   @Override
   public XSiteResponse backupRemotely(XSiteBackup backup, XSiteReplicateCommand rpcCommand) {
      DummyXSiteResponse response = new DummyXSiteResponse(backup, fail);
      response.complete();
      return response;
   }

   @Override
   public Log getLog() {
      return actual.getLog();
   }

   private static class DummyXSiteResponse extends CompletableFuture<Void> implements XSiteResponse {

      private final XSiteBackup backup;
      private final boolean fail;

      private DummyXSiteResponse(XSiteBackup backup, boolean fail) {
         this.backup = backup;
         this.fail = fail;
      }

      @Override
      public void whenCompleted(XSiteResponseCompleted xSiteResponseCompleted) {
         xSiteResponseCompleted
               .onCompleted(backup, System.currentTimeMillis(), 0, fail ? new TimeoutException() : null);
      }

      void complete() {
         if (fail) {
            completeExceptionally(new TimeoutException());
         } else {
            complete(null);
         }
      }
   }
}
