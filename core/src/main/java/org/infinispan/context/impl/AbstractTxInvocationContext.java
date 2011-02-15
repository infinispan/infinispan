package org.infinispan.context.impl;

import org.infinispan.CacheException;
import org.infinispan.transaction.xa.AbstractCacheTransaction;

import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Support class for {@link org.infinispan.context.impl.TxInvocationContext}.
 *
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 * @since 4.0
 */
public abstract class AbstractTxInvocationContext extends AbstractInvocationContext implements TxInvocationContext {

   public boolean hasModifications() {
      return getModifications() != null && !getModifications().isEmpty();
   }

   public Set<Object> getAffectedKeys() {
      return getCacheTrasaction().getAffectedKeys();
   }

   public void addAffectedKeys(Collection<Object> keys) {
      if (keys != null && !keys.isEmpty()) {
         Set<Object> affectedKeys = getCacheTrasaction().getAffectedKeys();
         if (affectedKeys == null || affectedKeys.isEmpty()) {
            affectedKeys = new HashSet<Object>();
         }
         affectedKeys.addAll(keys);
         getCacheTrasaction().setAffectedKeys(affectedKeys);
      }
   }

   public boolean isInTxScope() {
      return true;
   }

   public abstract AbstractCacheTransaction getCacheTrasaction();

}
