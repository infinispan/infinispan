package org.infinispan.interceptors.impl;

import static org.infinispan.persistence.manager.PersistenceManager.AccessMode.BOTH;
import static org.infinispan.persistence.manager.PersistenceManager.AccessMode.PRIVATE;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Handles store write operations when passivation enabled that don't entail reading the entry first
 *
 * @author William Burns
 * @since 9.0
 */
public class PassivationWriterInterceptor extends DDAsyncInterceptor {
   private final boolean trace = getLog().isTraceEnabled();
   @Inject protected PersistenceManager persistenceManager;

   private static final Log log = LogFactory.getLog(PassivationWriterInterceptor.class);

   protected Log getLog() {
      return log;
   }

   @Override
   public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      if (isStoreEnabled(command) && !ctx.isInTxScope())
         persistenceManager.clearAllStores(ctx.isOriginLocal() ? BOTH : PRIVATE);

      return invokeNext(ctx, command);
   }

   protected boolean isStoreEnabled(FlagAffectedCommand command) {
      if (command.hasAnyFlag(FlagBitSets.SKIP_CACHE_STORE)) {
         if (trace) {
            log.trace("Skipping cache store since the call contain a skip cache store flag");
         }
         return false;
      }
      return true;
   }
}
