package org.infinispan.interceptors.distribution;

import org.infinispan.remoting.RpcException;
import org.infinispan.remoting.responses.BiasRevocationResponse;
import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.ValidResponse;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.ResponseCollector;
import org.infinispan.remoting.transport.ResponseCollectors;

public interface BiasedCollector extends Collector<ValidResponse>, ResponseCollector<ValidResponse> {
   void addPendingAcks(boolean success, Address[] waitFor);

   @Override
   default ValidResponse addResponse(Address sender, Response response) {
      if (response instanceof ValidResponse) {
         if (response instanceof BiasRevocationResponse) {
            addPendingAcks(response.isSuccessful(), ((BiasRevocationResponse) response).getWaitList());
         }
         ValidResponse valid = (ValidResponse) response;
         primaryResult(valid, response.isSuccessful());
         return valid;
      } else if (response instanceof ExceptionResponse) {
         primaryException(ResponseCollectors.wrapRemoteException(sender, ((ExceptionResponse) response).getException()));
      } else if (response instanceof CacheNotFoundResponse) {
         primaryException(ResponseCollectors.remoteNodeSuspected(sender));
      } else {
         primaryException(new RpcException("Unknown response type: " + response));
      }
      // There won't be any further targets so finish() will be called
      return null;
   }

   @Override
   default ValidResponse finish() {
      return null;
   }
}
