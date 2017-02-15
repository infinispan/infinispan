package org.infinispan.transaction.tm;

import javax.transaction.xa.XAResource;

import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Simple transaction manager implementation that maintains transaction state in memory only.
 * <p>
 * See {@link EmbeddedBaseTransactionManager} for details about which features are supported.
 *
 * @author bela
 * @author Pedro Ruivo
 * @see EmbeddedBaseTransactionManager
 * @since 9.0
 */
public class EmbeddedTransactionManager extends EmbeddedBaseTransactionManager {

   protected static final Log log = LogFactory.getLog(EmbeddedTransactionManager.class);

   private EmbeddedTransactionManager() {
   }

   public static EmbeddedTransactionManager getInstance() {
      return LazyInitializeHolder.TM_INSTANCE;
   }

   public static EmbeddedUserTransaction getUserTransaction() {
      return LazyInitializeHolder.USER_TX_INSTANCE;
   }

   public static void destroy() {
      dissociateTransaction();
   }

   public XAResource firstEnlistedResource() {
      return getTransaction().firstEnlistedResource();
   }

   private static class LazyInitializeHolder {
      static final EmbeddedTransactionManager TM_INSTANCE = new EmbeddedTransactionManager();
      static final EmbeddedUserTransaction USER_TX_INSTANCE = new EmbeddedUserTransaction(TM_INSTANCE);
   }
}
