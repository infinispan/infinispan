package org.infinispan.xsite;

import java.util.Map;

import javax.transaction.Transaction;

/**
 * @author Mircea Markus
 * @since 5.2
 */
public class CountingCustomFailurePolicy extends AbstractCustomFailurePolicy {

   public static volatile boolean PUT_INVOKED;
   public static volatile boolean REMOVE_INVOKED;
   public static volatile boolean REPLACE_INVOKED;
   public static volatile boolean CLEAR_INVOKED;
   public static volatile boolean PUT_ALL_INVOKED;
   public static volatile boolean PREPARE_INVOKED;
   public static volatile boolean ROLLBACK_INVOKED;
   public static volatile boolean COMMIT_INVOKED;

   @Override
   public void handlePutFailure(String site, Object key, Object value, boolean putIfAbsent) {
      PUT_INVOKED = true;
   }

   @Override
   public void handleRemoveFailure(String site, Object key, Object oldValue) {
      REMOVE_INVOKED = true;
   }

   @Override
   public void handleReplaceFailure(String site, Object key, Object oldValue, Object newValue) {
      REPLACE_INVOKED = true;
   }

   @Override
   public void handleClearFailure(String site) {
      CLEAR_INVOKED = true;
   }

   @Override
   public void handlePutAllFailure(String site, Map map) {
      PUT_ALL_INVOKED = true;
   }

   @Override
   public void handlePrepareFailure(String site, Transaction transaction) {
      if (transaction == null)
         throw new IllegalStateException();
      PREPARE_INVOKED = true;
      throw new BackupFailureException();
   }

   @Override
   public void handleRollbackFailure(String site, Transaction transaction) {
      ROLLBACK_INVOKED = true;
   }

   @Override
   public void handleCommitFailure(String site, Transaction transaction) {
      COMMIT_INVOKED = true;
   }
}
