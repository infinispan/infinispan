package org.infinispan.hotrod.configuration;

/**
 * Specifies how a resource is enlisted in a transaction
 *
 * If {@link #NONE} is used, the resource won't be transactional.
 *
 * @since 14.0
 */
public enum TransactionMode {
   /**
    * The cache is not transactional
    */
   NONE,
   /**
    * The cache is enlisted as a synchronization
    */
   NON_XA,
   /**
    * The cache is enlisted as XAResource but it doesn't keep any recovery information.
    */
   NON_DURABLE_XA,
   /**
    * The cache is enlisted as XAResource with recovery support.
    *
    * This mode isn't available yet.
    */
   FULL_XA
}
