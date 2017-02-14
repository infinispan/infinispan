package org.infinispan.transaction.tm;

/**
 * Not really a transaction manager in the truest sense of the word.  Only used to batch up operations.  Proper
 * transactional semantics of rollbacks and recovery are NOT used here.
 *
 * @author bela
 * @since 4.0
 */
public class BatchModeTransactionManager extends EmbeddedBaseTransactionManager {

   private static BatchModeTransactionManager INSTANCE = null;

   private BatchModeTransactionManager() {
   }

   public static BatchModeTransactionManager getInstance() {
      if (INSTANCE == null) {
         INSTANCE = new BatchModeTransactionManager();
      }
      return INSTANCE;
   }

   public static void destroy() {
      if (INSTANCE == null) {
         return;
      }
      dissociateTransaction();
      INSTANCE = null;
   }

}
