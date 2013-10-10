package org.infinispan.context.impl;

import org.infinispan.commands.write.WriteCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.transaction.xa.CacheTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;

import javax.transaction.Transaction;
import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * Interface defining additional functionality for invocation contexts that propagate within a transaction's scope.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public interface TxInvocationContext extends InvocationContext {

   /**
    * Checks if there are modifications performed within the tx's scope. Any modifications having Flag.CACHE_MODE_LOCAL are ignored.
    */
   boolean hasModifications();

   /**
    * Returns the set of keys that are affected by this transaction.  Used to generate appropriate recipient groups
    * for cluster-wide prepare and commit calls.
    */
   Set<Object> getAffectedKeys();

   /**
    * Returns the id of the transaction associated  with the current call.
    */
   GlobalTransaction getGlobalTransaction();

   /**
    * Returns the modifications performed in the scope of the current transaction. Any modifications having Flag.CACHE_MODE_LOCAL are ignored.
    * The returned list can be null.
    */
   List<WriteCommand> getModifications();

   /**
    * Returns the tx associated with the current thread. This method MUST be guarded with a call to {@link
    * #isOriginLocal()}, as {@link javax.transaction.Transaction} are not propagated from the node where tx was
    * started.
    * @throws IllegalStateException if the call is performed from a {@link #isOriginLocal()}==false context.
    */
   Transaction getTransaction();

   /**
    * Registers a new participant with the transaction.
    */
   void addAllAffectedKeys(Collection<Object> keys);

   void addAffectedKey(Object key);

   /**
    *
    * @return true if the current transaction is in a valid state to perform operations on (i.e.,RUNNING or PREPARING)
    * or false otherwise.
    */
   boolean isTransactionValid();

   /**
    * Marks this transaction as implicit; implicit transactions are started for transactional caches that have the autoCommit enabled.
    */
   void setImplicitTransaction(boolean implicit);


   boolean isImplicitTransaction();

   CacheTransaction getCacheTransaction();

   void skipTransactionCompleteCheck(boolean skip);

   boolean skipTransactionCompleteCheck();
}
