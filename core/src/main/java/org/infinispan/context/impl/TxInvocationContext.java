package org.infinispan.context.impl;

import org.infinispan.commands.write.WriteCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.xa.GlobalTransaction;

import javax.transaction.Transaction;
import java.util.List;
import java.util.Set;

/**
 * // TODO: Mircea: Document this!
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public interface TxInvocationContext extends InvocationContext {

   public boolean hasModifications();

   Set<Address> getTransactionParticipants();

   GlobalTransaction getClusterTransactionId();

   List<WriteCommand> getModifications();

   Transaction getRunningTransaction();

   boolean isValidRunningTx();

   void addTransactionParticipants(List<Address> addresses);
}
