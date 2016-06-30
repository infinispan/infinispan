package org.infinispan.remoting.rpc;

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
   /**
    * Most commands should use this mode to prevent SuspectExceptions when we are doing a broadcast
    * (or anycast that translates to JGroups broadcast). That would cause SuspectExceptions in SYNCHRONOUS mode
    * in a situation when:
    * 1) node is leaving, we want to address all living members but while topology was already updated, view was not yet
    * 2) we use asymmetric cluster so the other nodes respond with CacheNotFoundResponse to such broadcast
    */
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
