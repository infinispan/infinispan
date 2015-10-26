package org.infinispan.interceptors.distribution;

import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.base.SequentialInterceptor;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.impl.LocalTransaction;
import org.infinispan.transaction.xa.CacheTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collection;
import java.util.Map;

import static org.infinispan.transaction.impl.WriteSkewHelper.readVersionsFromResponse;

/**
 * A version of the {@link TxDistributionInterceptor} that adds logic to handling prepares when entries are versioned.
 *
 * @author Manik Surtani
 * @deprecated Since 8.1, use {@link org.infinispan.interceptors.sequential.VersionedDistributionInterceptor} instead.
 */
@Deprecated
public class VersionedDistributionInterceptor extends TxDistributionInterceptor {
   @Override
   public Class<? extends SequentialInterceptor> getSequentialInterceptor() {
      return org.infinispan.interceptors.sequential.VersionedDistributionInterceptor.class;
   }
}
