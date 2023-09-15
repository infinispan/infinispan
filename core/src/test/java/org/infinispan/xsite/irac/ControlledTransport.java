package org.infinispan.xsite.irac;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.remoting.transport.AbstractDelegatingTransport;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.XSiteResponse;
import org.infinispan.xsite.XSiteBackup;
import org.infinispan.xsite.commands.remote.XSiteRequest;

public class ControlledTransport extends AbstractDelegatingTransport {

   volatile Supplier<Throwable> throwableSupplier = () -> null;
   private final String local;
   private final Collection<String> connectedSites;
   private final Collection<String> disconnectedSites;

   ControlledTransport(Transport actual,
                       String local,
                       Collection<String> connectedSites,
                       Collection<String> disconnectedSites) {
      super(actual);
      this.local = local;
      this.connectedSites = connectedSites;
      this.disconnectedSites = disconnectedSites;
   }

   ControlledTransport(Transport actual, String local, Collection<String> disconnectedSites) {
      this(actual, local, Collections.singleton(local), disconnectedSites);
   }

   @Override
   public void start() {
      //already started
   }

   @Override
   public <O> XSiteResponse<O> backupRemotely(XSiteBackup backup, XSiteRequest<O> rpcCommand) {
      Throwable t = null;
      if (disconnectedSites.contains(backup.getSiteName())) t = throwableSupplier.get();
      ControlledXSiteResponse<O> response = new ControlledXSiteResponse<>(backup, t);
      response.complete();
      return response;
   }

   @Override
   public void checkCrossSiteAvailable() throws CacheConfigurationException {
      //no-op == it is available
   }

   @Override
   public String localSiteName() {
      return local;
   }

   @Override
   public Set<String> getSitesView() {
      return Set.copyOf(connectedSites);
   }

   private static class ControlledXSiteResponse<T> extends CompletableFuture<T> implements XSiteResponse<T> {

      private final XSiteBackup backup;
      private final Throwable result;

      private ControlledXSiteResponse(XSiteBackup backup, Throwable result) {
         this.backup = backup;
         this.result = result;
      }

      @Override
      public void whenCompleted(XSiteResponseCompleted listener) {
         listener.onCompleted(backup, System.currentTimeMillis(), 0, result);
      }

      void complete() {
         if (result == null) {
            complete(null);
         } else {
            completeExceptionally(result);
         }
      }
   }
}
