package org.infinispan.util.concurrent;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Ordering construct allowing concurrent operations that wish to do operations upon the same key to wait until
 * the most recently registered operation is complete in a non blocking way.
 * @author wburns
 * @since 10.0
 */
public class DataOperationOrderer {
   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());

   private final ConcurrentMap<Object, CompletionStage<Operation>> objectStages = new ConcurrentHashMap<>();

   /**
    * Returns how many keys have pending operations waiting for their completion
    * @return number of keys with pending operations
    */
   public int pendingOperations() {
      return objectStages.size();
   }

   public enum Operation {
      READ,
      REMOVE,
      WRITE
   }

   /**
    * Registers the provided Stage to be next in line to do an operation on behalf of the provided key.
    * Returns a different Stage that when complete signals that this operation should continue or null if there
    * is no wait required.
    * @param key delineating identifier for an operation
    * @param register stage to register for others to wait upon for future registrations
    * @return stage that signals when the operation that is registering its own future may continue or null if nothing
    *         to wait on
    */
   public CompletionStage<Operation> orderOn(Object key, CompletionStage<Operation> register) {
      CompletionStage<Operation> current = objectStages.put(key, register);
      if (log.isTraceEnabled()) {
         log.tracef("Ordering upcoming future %s for key %s to run after %s", register, key, current);
      }
      return current;
   }

   /**
    * Completes a given operation and removes all internal references from the orderer
    * @param key delineating identifier for an operation
    * @param registeredFuture previously registered future that is removed from memory as needed
    * @param operation the type of operation
    */
   public void completeOperation(Object key, CompletableFuture<Operation> registeredFuture, Operation operation) {
      if (log.isTraceEnabled()) {
         log.tracef("Ordered future %s is completed for key %s from op %s", registeredFuture, key, operation);
      }
      // If nothing was removed that is fine - means another operation has been registered
      objectStages.remove(key, registeredFuture);
      registeredFuture.complete(operation);
   }

   /**
    * For testing purposes only
    */
   public CompletionStage<Operation> getCurrentStage(Object key) {
      return objectStages.get(key);
   }
}
