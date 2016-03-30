package org.infinispan.transaction.tm;

/**
 * Not really a transaction manager in the truest sense of the word.  Only used to batch up operations.  Proper
 * transactional semantics of rollbacks and recovery are NOT used here.
 *
 * @author bela
 * @since 4.0
 */
public class BatchModeTransactionManager extends DummyBaseTransactionManager {
   
   private static final long serialVersionUID = 5656602677430350961L;

   static BatchModeTransactionManager instance = null;

   public static BatchModeTransactionManager getInstance() {
      if (instance == null) {
         instance = new BatchModeTransactionManager();
      }
      return instance;
   }

   public static void destroy() {
      if (instance == null) return;
      instance.setTransaction(null);
      instance = null;
   }

}
