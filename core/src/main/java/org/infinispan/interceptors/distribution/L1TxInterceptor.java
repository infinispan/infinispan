package org.infinispan.interceptors.distribution;

import org.infinispan.interceptors.base.SequentialInterceptor;

/**
 * Interceptor that handles L1 logic for transactional caches.
 *
 * @author William Burns
 * @deprecated Since 8.1, use {@link org.infinispan.interceptors.sequential.L1TxInterceptor} instead.
 */
@Deprecated
public class L1TxInterceptor extends L1NonTxInterceptor {
   @Override
   public Class<? extends SequentialInterceptor> getSequentialInterceptor() {
      return org.infinispan.interceptors.sequential.L1TxInterceptor.class;
   }
}
