package org.infinispan.interceptors.distribution;

import org.infinispan.interceptors.base.BaseRpcInterceptor;
import org.infinispan.interceptors.base.SequentialInterceptor;

/**
 * Interceptor that handles L1 logic for non-transactional caches.
 *
 * @author Mircea Markus
 * @author William Burns
 * @deprecated Since 8.1, use {@link org.infinispan.interceptors.sequential.L1NonTxInterceptor} instead.
 */
@Deprecated
public class L1NonTxInterceptor extends BaseRpcInterceptor {
   @Override
   public Class<? extends SequentialInterceptor> getSequentialInterceptor() {
      return org.infinispan.interceptors.sequential.L1NonTxInterceptor.class;
   }
}
