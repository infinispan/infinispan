package org.infinispan.context.impl;

import org.infinispan.CacheException;

import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import java.util.Arrays;
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

   public boolean hasModifications() {
      return getModifications() != null && !getModifications().isEmpty();
   }

   public Set<Object> getAffectedKeys() {
      return affectedKeys == null ? Collections.emptySet() : affectedKeys;
   }

   public void addAffectedKeys(Object... keys) {
      if (affectedKeys == null) {
         affectedKeys = new HashSet<Object>();
      }
      affectedKeys.addAll(Arrays.asList(keys));
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
