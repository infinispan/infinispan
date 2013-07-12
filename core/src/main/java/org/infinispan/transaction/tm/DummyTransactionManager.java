package org.infinispan.transaction.tm;

import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.transaction.xa.XAResource;

/**
 * Simple transaction manager implementation that maintains transaction state in memory only.
 *
 * @author bela
 *         <p/>
 *         Date: May 15, 2003 Time: 4:11:37 PM
 * @since 4.0
 */
public class DummyTransactionManager extends DummyBaseTransactionManager {

   protected static final Log log = LogFactory.getLog(DummyTransactionManager.class);

   private static final long serialVersionUID = 4396695354693176535L;

   public static DummyTransactionManager getInstance() {
      return LazyInitializeHolder.dummyTMInstance;
   }

   public static DummyUserTransaction getUserTransaction() {
      return LazyInitializeHolder.utx;
   }

   public static void destroy() {
      setTransaction(null);
   }

   public XAResource firstEnlistedResource() {
      return getTransaction().firstEnlistedResource();
   }

   private static class LazyInitializeHolder {
      static final DummyTransactionManager dummyTMInstance = new DummyTransactionManager();
      static final DummyUserTransaction utx = new DummyUserTransaction(dummyTMInstance);
   }
}
