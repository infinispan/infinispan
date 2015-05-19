package org.infinispan.remoting.rpc;

import org.infinispan.configuration.cache.Configuration;

/**
 * Represents different handling mechanisms when dealing with remote command responses.
 * These include waiting for responses from all nodes in the cluster ({@link ResponseMode#SYNCHRONOUS}}),
 * not waiting for any responses at all ({@link ResponseMode#ASYNCHRONOUS}}),
 * or waiting for first valid response ({@link ResponseMode#WAIT_FOR_VALID_RESPONSE}})
 *
 * @author Manik Surtani
 * @since 4.0
 */
public enum ResponseMode {
   SYNCHRONOUS,
   SYNCHRONOUS_IGNORE_LEAVERS,
   ASYNCHRONOUS,
   WAIT_FOR_VALID_RESPONSE;

   public boolean isSynchronous() {
      return !isAsynchronous();
   }

   public boolean isAsynchronous() {
      return this == ASYNCHRONOUS;
   }
}
