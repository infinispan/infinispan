package org.infinispan.interceptors.totalorder;

import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.configuration.cache.Configurations;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.distribution.TxDistributionInterceptor;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.impl.LocalTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collection;

/**
 * This interceptor handles distribution of entries across a cluster, as well as transparent lookup, when the total
 * order based protocol is enabled
 *
 * @author Pedro Ruivo
 * @deprecated Since 8.1, use {@link org.infinispan.interceptors.sequential.totalorder.TotalOrderDistributionInterceptor} instead.
 */
@Deprecated
public class TotalOrderDistributionInterceptor extends TxDistributionInterceptor {

   private static final Log log = LogFactory.getLog(TotalOrderDistributionInterceptor.class);

   @Override
   protected Log getLog() {
      return log;
   }
}
