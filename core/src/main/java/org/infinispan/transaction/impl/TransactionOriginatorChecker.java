package org.infinispan.transaction.impl;

import java.util.Collection;

import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.xa.GlobalTransaction;

/**
 * An interface to check if the transaction originator is left or not.
 *
 * @author Pedro Ruivo
 * @since 9.1
 */
@Scope(Scopes.NAMED_CACHE)
public interface TransactionOriginatorChecker {

   /**
    * A Local mode implementation.
    */
   TransactionOriginatorChecker LOCAL = new TransactionOriginatorChecker() {
      @Override
      public boolean isOriginatorMissing(GlobalTransaction gtx) {
         return false;
      }

      @Override
      public boolean isOriginatorMissing(GlobalTransaction gtx, Collection<Address> liveMembers) {
         return false;
      }
   };

   /**
    * @return {@code true} if the member who executed {@code gtx} is missing.
    */
   boolean isOriginatorMissing(GlobalTransaction gtx);

   /**
    * @return {@code true} if the member who executed {@code gtx} is missing using the {@code liveMembers}.
    */
   boolean isOriginatorMissing(GlobalTransaction gtx, Collection<Address> liveMembers);

}
