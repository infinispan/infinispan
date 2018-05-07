package org.infinispan.hibernate.cache.commons.access;

import javax.transaction.Status;
import javax.transaction.Synchronization;

import org.infinispan.hibernate.cache.commons.util.InfinispanMessageLogger;

public class LocalInvalidationSynchronization implements Synchronization {
   private final static InfinispanMessageLogger log = InfinispanMessageLogger.Provider.getLog(LocalInvalidationSynchronization.class);
   private final static boolean trace = log.isTraceEnabled();

   private final Object lockOwner;
   private final PutFromLoadValidator validator;
   private final Object key;

   public LocalInvalidationSynchronization(PutFromLoadValidator validator, Object key, Object lockOwner) {
      assert lockOwner != null;
      this.validator = validator;
      this.key = key;
      this.lockOwner = lockOwner;
   }

   @Override
   public void beforeCompletion() {}

   @Override
   public void afterCompletion(int status) {
      if (trace) {
         log.tracef("After completion callback with status %d", status);
      }
      validator.endInvalidatingKey(lockOwner, key, status == Status.STATUS_COMMITTED || status == Status.STATUS_COMMITTING);
   }
}
