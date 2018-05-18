package org.infinispan.client.hotrod.configuration;

import javax.transaction.Synchronization;
import javax.transaction.Transaction;
import javax.transaction.xa.XAResource;

import org.infinispan.client.hotrod.RemoteCache;

/**
 * Specifies how the {@link RemoteCache} is enlisted in the {@link Transaction}.
 *
 * If {@link #NONE} is used, the {@link RemoteCache} won't be transactional.
 *
 * @author Pedro Ruivo
 * @since 9.3
 */
public enum TransactionMode {
   /**
    * The cache is not transactional
    */
   NONE,
   /**
    * The cache is enlisted as {@link Synchronization}.
    */
   NON_XA,
   /**
    * The cache is enlisted as {@link XAResource} but it doesn't keep any recovery information.
    */
   NON_DURABLE_XA,
   /**
    * The cache is enlisted as{@link XAResource} with recovery support.
    *
    * This mode isn't available yet.
    */
   FULL_XA
}
