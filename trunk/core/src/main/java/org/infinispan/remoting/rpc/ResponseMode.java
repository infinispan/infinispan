package org.infinispan.remoting.rpc;

/**
 * Represents different handling mechanisms when dealing with remote command responses. 
 * These include waiting for responses from all nodes in the cluster ({@link ResponseMode#SYNCHRONOUS}}),
 * not waiting for any responses at all ({@link ResponseMode#ASYNCHRONOUS}} or 
 * {@link ResponseMode#ASYNCHRONOUS_WITH_SYNC_MARSHALLING}}), or waiting for first valid response 
 * ({@link ResponseMode#WAIT_FOR_VALID_RESPONSE}})
 *
 * @author Manik Surtani
 * @since 4.0
 */
public enum ResponseMode {
   SYNCHRONOUS,
   ASYNCHRONOUS,
   ASYNCHRONOUS_WITH_SYNC_MARSHALLING,
   WAIT_FOR_VALID_RESPONSE;

   public boolean isAsynchronous() {
      return this == ASYNCHRONOUS || this == ASYNCHRONOUS_WITH_SYNC_MARSHALLING;
   }
}
