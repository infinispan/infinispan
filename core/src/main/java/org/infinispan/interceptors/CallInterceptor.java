package org.infinispan.interceptors;


import java.util.Map;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.read.GetCacheEntryCommand;
import org.infinispan.commands.read.GetAllCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.interceptors.base.SequentialInterceptor;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Always at the end of the chain, directly in front of the cache. Simply calls into the cache using reflection. If the
 * call resulted in a modification, add the Modification to the end of the modification list keyed by the current
 * transaction.
 *
 * @author Bela Ban
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 * @deprecated Since 8.1, use {@link org.infinispan.configuration.cache.InterceptorConfiguration.Position#LAST}
 */
public class CallInterceptor extends CommandInterceptor {
   private static final Log log = LogFactory.getLog(CallInterceptor.class);

   @Override
   protected Log getLog() {
      return log;
   }

   @Override
   final public Object handleDefault(InvocationContext ctx, VisitableCommand command) throws Throwable {
      return null;
   }

   @Override
   public Class<? extends SequentialInterceptor> getSequentialInterceptor() {
      return org.infinispan.interceptors.sequential.CallInterceptor.class;
   }
}