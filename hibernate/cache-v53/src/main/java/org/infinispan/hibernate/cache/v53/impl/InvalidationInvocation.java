/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.infinispan.hibernate.cache.v53.impl;

import java.util.concurrent.CompletableFuture;

import org.infinispan.hibernate.cache.commons.access.NonTxPutFromLoadInterceptor;
import org.infinispan.hibernate.cache.commons.util.InfinispanMessageLogger;

/**
 * Synchronization that should release the locks after invalidation is complete.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class InvalidationInvocation implements Invocation {
   private final static InfinispanMessageLogger log = InfinispanMessageLogger.Provider.getLog(InvalidationInvocation.class);
   public final Object lockOwner;
   private final NonTxPutFromLoadInterceptor nonTxPutFromLoadInterceptor;
   private final Object key;

   public InvalidationInvocation(NonTxPutFromLoadInterceptor nonTxPutFromLoadInterceptor, Object key, Object lockOwner) {
      assert lockOwner != null;
      this.nonTxPutFromLoadInterceptor = nonTxPutFromLoadInterceptor;
      this.key = key;
      this.lockOwner = lockOwner;
   }

   @Override
   public CompletableFuture<Void> invoke(boolean success) {
      log.tracef("After completion callback, success? %s", success);
      nonTxPutFromLoadInterceptor.endInvalidating(key, lockOwner, success);
      return null;
   }
}
