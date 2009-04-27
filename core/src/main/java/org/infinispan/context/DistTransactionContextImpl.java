package org.infinispan.context;

import org.infinispan.remoting.transport.Address;

import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * A transaction context that adds behavior specific to DIST
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class DistTransactionContextImpl extends TransactionContextImpl {

   final Set<Address> participants = new HashSet<Address>();

   public DistTransactionContextImpl(Transaction tx) throws SystemException, RollbackException {
      super(tx);
   }

   @Override
   public Set<Address> getTransactionParticipants() {
      return participants;
   }

   @Override
   public void addTransactionParticipants(Collection<Address> addresses) {
      participants.addAll(addresses);
   }

   @Override
   public void reset() {
      super.reset();
      participants.clear();
   }
}
