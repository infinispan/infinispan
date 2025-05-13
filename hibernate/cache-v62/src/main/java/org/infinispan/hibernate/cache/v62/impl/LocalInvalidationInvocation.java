package org.infinispan.hibernate.cache.v62.impl;

import java.util.concurrent.CompletableFuture;

import org.infinispan.hibernate.cache.commons.access.PutFromLoadValidator;
import org.infinispan.hibernate.cache.commons.util.InfinispanMessageLogger;

/**
 * Synchronization that should release the locks after invalidation is complete.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class LocalInvalidationInvocation implements Invocation {
   private static final InfinispanMessageLogger log = InfinispanMessageLogger.Provider.getLog(LocalInvalidationInvocation.class);

   private final Object lockOwner;
   private final PutFromLoadValidator validator;
   private final Object key;

   public LocalInvalidationInvocation(PutFromLoadValidator validator, Object key, Object lockOwner) {
      assert lockOwner != null;
      this.validator = validator;
      this.key = key;
      this.lockOwner = lockOwner;
   }

   @Override
   public CompletableFuture<Void> invoke(boolean success) {
      if (log.isTraceEnabled()) {
         log.tracef("After completion callback, success=%b", success);
      }
      validator.endInvalidatingKey(lockOwner, key, success);
      return null;
   }
}
