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
import org.infinispan.xsite.commands.remote.XSiteRequest;

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
   public BackupResponse backupRemotely(final Collection<XSiteBackup> backups, XSiteRequest<?> rpcCommand) {
      throw new UnsupportedOperationException();
   }

   @Override
   public <O> XSiteResponse<O> backupRemotely(XSiteBackup backup, XSiteRequest<O> rpcCommand) {
      DummyXSiteResponse<O> response = new DummyXSiteResponse<>(backup, fail);
      response.complete();
      return response;
   }

   @Override
   public Log getLog() {
      return actual.getLog();
   }

   private static class DummyXSiteResponse<O> extends CompletableFuture<O> implements XSiteResponse<O> {

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
