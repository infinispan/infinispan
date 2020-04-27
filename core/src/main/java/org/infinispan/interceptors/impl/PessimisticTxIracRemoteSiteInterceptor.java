package org.infinispan.interceptors.impl;

import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.context.InvocationContext;
import org.infinispan.distribution.DistributionInfo;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Interceptor for pessimsitic transactional caches to handle updates from remote sites.
 * <p>
 * Since the pessimistic transactions commits in one phase and the version need to be validated after the lock is
 * acquired, the other site update must be "replayed" in the primary owner.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
public class PessimisticTxIracRemoteSiteInterceptor extends AbstractIracRemoteSiteInterceptor {

   private static final Log log = LogFactory.getLog(PessimisticTxIracRemoteSiteInterceptor.class);
   private static final boolean trace = log.isTraceEnabled();

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) {
      return visitDataWriteCommand(ctx, command);
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) {
      return visitDataWriteCommand(ctx, command);
   }

   @Override
   boolean isTraceEnabled() {
      return trace;
   }

   @Override
   Log getLog() {
      return log;
   }

   private Object visitDataWriteCommand(InvocationContext ctx, DataWriteCommand command) {
      if (isNormalWriteCommand(command)) {
         return invokeNext(ctx, command);
      }

      final Object key = command.getKey();
      DistributionInfo dInfo = getDistributionInfo(key);
      //this is a "limitation" with pessimistic transactions
      //the tx is committed in a single phase so the SiteMaster needs to forward the update to the primary owner
      //then, the tx will run locally on the primary owner and it can be validated
      if (ctx.isOriginLocal()) {
         if (dInfo.isPrimary()) {
            validateOnPrimary(ctx, command, null);
            //at this point, the lock is acquired.
            //If the update is discarded (command.isSuccessful() == false) it will not be enlisted in the tx
            //the tx will be read-only and a successful ack will be sent to the original site.
            return command.isSuccessful() ? invokeNext(ctx, command) : null;
         } else {
            //the ClusteredCacheBackupReceiver sends the forwards the request to the primary owner,
            //but the topology can change. If that happens, send back an exception to the other site to be retried later.
            throw new CacheException("Update must be executed in the primary owner!", null, false, false);
         }
      } else {
         if (dInfo.isWriteOwner()) {
            setIracMetadataForOwner(ctx, command, null);
         }
      }

      return invokeNext(ctx, command);
   }
}
