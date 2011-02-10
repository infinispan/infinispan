package org.infinispan.context.impl;

import javax.transaction.Transaction;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Support class for {@link org.infinispan.context.impl.TxInvocationContext}.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public abstract class AbstractTxInvocationContext extends AbstractInvocationContext implements TxInvocationContext {

   protected Set<Object> affectedKeys = null;
   private Transaction transaction;

   public boolean hasModifications() {
      return getModifications() != null && !getModifications().isEmpty();
   }

   public Set<Object> getAffectedKeys() {
      return affectedKeys == null ? Collections.emptySet() : affectedKeys;
   }

   public void addAffectedKeys(Collection<Object> keys) {
      if (keys != null && !keys.isEmpty()) {
         if (affectedKeys == null) {
            affectedKeys = new HashSet<Object>();
         }
         affectedKeys.addAll(keys);
      }
   }

   public boolean isInTxScope() {
      return true;
   }

   public TxInvocationContext setTransaction(Transaction transaction) {
      this.transaction = transaction;
      return this;
   }

   public Transaction getTransaction() {
      return transaction;
   }

   @Override
   public AbstractTxInvocationContext clone() {
      AbstractTxInvocationContext dolly = (AbstractTxInvocationContext) super.clone();
      if (this.affectedKeys != null) {
         dolly.affectedKeys = new HashSet<Object>(affectedKeys);
      }
      return dolly;
   }
}
