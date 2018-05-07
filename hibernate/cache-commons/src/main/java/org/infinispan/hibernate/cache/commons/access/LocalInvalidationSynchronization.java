package org.infinispan.hibernate.cache.commons.access;

import javax.transaction.Status;
import javax.transaction.Synchronization;

public class LocalInvalidationSynchronization implements Synchronization {
   public final Object lockOwner;
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
      validator.endInvalidatingKey(key, lockOwner, status == Status.STATUS_COMMITTED || status == Status.STATUS_COMMITTING);
   }
}
