package org.infinispan.remoting.rpc;

/**
 * // TODO: Manik: Document this!
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
