package org.infinispan.xsite.response;

import org.infinispan.configuration.cache.XSiteStateTransferMode;
import org.infinispan.remoting.RpcException;
import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.ResponseCollector;
import org.infinispan.remoting.transport.ResponseCollectors;

/**
 * A {@link ResponseCollector} that merges {@link AutoStateTransferResponse}.
 *
 * @author Pedro Ruivo
 * @since 12.1
 */
public class AutoStateTransferResponseCollector implements ResponseCollector<AutoStateTransferResponse> {

   private boolean isOffline;
   private XSiteStateTransferMode stateTransferMode;

   public AutoStateTransferResponseCollector(boolean isOffline, XSiteStateTransferMode stateTransferMode) {
      this.isOffline = isOffline;
      this.stateTransferMode = stateTransferMode;
   }

   @Override
   public synchronized AutoStateTransferResponse finish() {
      return new AutoStateTransferResponse(isOffline, stateTransferMode);
   }

   @Override
   public final AutoStateTransferResponse addResponse(Address sender, Response response) {
      if (response instanceof AutoStateTransferResponse) {
         merge((AutoStateTransferResponse) response);
      } else if (response instanceof ExceptionResponse) {
         throw ResponseCollectors.wrapRemoteException(sender, ((ExceptionResponse) response).getException());
      } else if (!(response instanceof CacheNotFoundResponse)) {
         throw ResponseCollectors.wrapRemoteException(sender, new RpcException("Unknown response type: " + response));
      }
      return null;
   }

   public synchronized void merge(AutoStateTransferResponse response) {
      //if one node is in offline mode, then the remote site is offline
      isOffline = (isOffline || response.isOffline());
      //if one node has auto state transfer disabled, then the full cluster has auto state transfer disabled.
      if (response.stateTransferMode() == XSiteStateTransferMode.MANUAL) {
         stateTransferMode = XSiteStateTransferMode.MANUAL;
      }
   }

}
