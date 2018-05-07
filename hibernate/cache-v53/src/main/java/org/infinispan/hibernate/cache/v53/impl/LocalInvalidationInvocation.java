/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.infinispan.hibernate.cache.v53.impl;

import java.util.concurrent.CompletableFuture;

import org.infinispan.hibernate.cache.commons.access.PutFromLoadValidator;
import org.infinispan.hibernate.cache.commons.util.InfinispanMessageLogger;

/**
 * Synchronization that should release the locks after invalidation is complete.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class LocalInvalidationInvocation implements Invocation {
   private final static InfinispanMessageLogger log = InfinispanMessageLogger.Provider.getLog(LocalInvalidationInvocation.class);
   private final static boolean trace = log.isTraceEnabled();

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
      if (trace) {
         log.tracef("After completion callback, success=%b", success);
      }
      validator.endInvalidatingKey(lockOwner, key, success);
      return null;
   }
}
