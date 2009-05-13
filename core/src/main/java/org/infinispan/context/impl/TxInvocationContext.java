package org.infinispan.context.impl;

import org.infinispan.commands.write.WriteCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.xa.GlobalTransaction;

import javax.transaction.Transaction;
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
    * Were there any modifications performed within the tx's scope?
    */
   public boolean hasModifications();

   /**
    * Returns the set of cluster participants (identified through {@link org.infinispan.remoting.transport.Address}
    * objects) that participate within the transaction. Null indicates all cluster members.
    */
   Set<Address> getTransactionParticipants();

   /**
    * Returns the id of the transaction assoctiated  with the current call.
    */
   GlobalTransaction getGlobalTransaction();

   /**
    * Returns all the modifications performed in the scope of the current transaction.
    */
   List<WriteCommand> getModifications();

   /**
    * Returns the tx associated with the current thread. This method MUST be guarded with a call to {@link
    * #isOriginLocal()}, as {@link javax.transaction.Transaction} are not propagated from the node where tx was
    * started.
    * @throws IllegalStateException if the call is performed from a {@link #isOriginLocal()}==false context.
    */
   Transaction getRunningTransaction();

   /**
    * Returns true if the current tx is either ACTIVE or PREPARING, false otherwise.
    */
   boolean isValidRunningTx();

   /**
    * Registers a new participant with the transaction.
    */
   void addTransactionParticipants(List<Address> addresses);
}
