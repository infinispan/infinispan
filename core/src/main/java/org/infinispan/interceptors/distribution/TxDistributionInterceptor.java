package org.infinispan.interceptors.distribution;

import org.infinispan.commands.write.WriteCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.base.SequentialInterceptor;

/**
 * Handles the distribution of the transactional caches.
 *
 * @author Mircea Markus
 * @deprecated Since 8.1, use {@link org.infinispan.interceptors.sequential.TxDistributionInterceptor} instead.
 */
@Deprecated
public class TxDistributionInterceptor extends BaseDistributionInterceptor {
   @Override
   protected boolean writeNeedsRemoteValue(InvocationContext ctx, WriteCommand command, Object key) {
      return false;
   }

   @Override
   protected void remoteGetBeforeWrite(InvocationContext ctx, WriteCommand command, Object key)
         throws Throwable {
   }

   @Override
   public Class<? extends SequentialInterceptor> getSequentialInterceptor() {
      return org.infinispan.interceptors.sequential.TxDistributionInterceptor.class;
   }
}
