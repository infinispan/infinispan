package org.infinispan.remoting.responses;

import org.infinispan.container.versioning.EntryVersionsMap;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Used in Total Order based commit protocol in Distributed Mode
 * <p/>
 * This filter awaits for one valid response for each key to be validated. This way, it avoids waiting for the reply of
 * all nodes involved in the transaction without compromise consistency
 *
 * @author Pedro Ruivo
 * @since 5.3
 */
public class KeysValidateFilter implements TimeoutValidationResponseFilter {

   private static final Log log = LogFactory.getLog(KeysValidateFilter.class);
   private final Address localAddress;
   private final Set<Object> keysAwaitingValidation;
   private boolean selfDelivered;

   public KeysValidateFilter(Address localAddress, Set<Object> keysAwaitingValidation) {
      this.localAddress = localAddress;
      this.keysAwaitingValidation = new HashSet<Object>(keysAwaitingValidation);
      this.selfDelivered = false;
   }

   @Override
   public synchronized boolean isAcceptable(Response response, Address sender) {
      if (sender.equals(localAddress)) {
         selfDelivered = true;
      }
      if (response instanceof SuccessfulResponse) {
         Object retVal = ((SuccessfulResponse) response).getResponseValue();
         if (retVal instanceof Collection<?>) {
            keysAwaitingValidation.removeAll((Collection<?>) retVal);
            if (log.isTraceEnabled()) {
               log.tracef("Received keys validated: %s. Awaiting validation of %s. Self Delivered? %s",
                          retVal, keysAwaitingValidation, selfDelivered);
            }
         } else if (retVal instanceof EntryVersionsMap) {
            keysAwaitingValidation.removeAll(((EntryVersionsMap) retVal).keySet());
            if (log.isTraceEnabled()) {
               log.tracef("Received keys validated: %s. Awaiting validation of %s. Self Delivered? %s",
                          ((EntryVersionsMap) retVal).keySet(), keysAwaitingValidation, selfDelivered);
            }
         } else {
            log.unexpectedResponse(retVal.getClass().toString(), "Collection or EntryVersionMap");
         }
      }
      return true;
   }

   @Override
   public synchronized boolean needMoreResponses() {
      return !selfDelivered || !keysAwaitingValidation.isEmpty();
   }

   @Override
   public synchronized void validate() throws TimeoutException {
      if (!selfDelivered) {
         throw new TimeoutException("Timeout waiting for member " + localAddress);
      } else if (!keysAwaitingValidation.isEmpty()) {
         throw new TimeoutException("Timeout waiting for the validation of keys: " + keysAwaitingValidation);
      }
   }
}
