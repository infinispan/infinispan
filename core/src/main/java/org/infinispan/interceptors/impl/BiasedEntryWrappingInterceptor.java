package org.infinispan.interceptors.impl;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.InvocationFinallyFunction;
import org.infinispan.scattered.BiasManager;

public class BiasedEntryWrappingInterceptor extends RetryingEntryWrappingInterceptor {
   private static final long NOT_BIASING_FLAGS = FlagBitSets.PUT_FOR_STATE_TRANSFER |
         FlagBitSets.PUT_FOR_X_SITE_STATE_TRANSFER | FlagBitSets.CACHE_MODE_LOCAL;

   private BiasManager biasManager;
   private final InvocationFinallyFunction handleDataWriteReturn = this::handleDataWriteReturn;
   private final InvocationFinallyFunction handleManyWriteReturn = this::handleManyWriteReturn;

   @Inject
   public void inject(BiasManager biasManager) {
      this.biasManager = biasManager;
   }

   @Override
   protected boolean canRead(Object key) {
      return biasManager.hasLocalBias(key) || super.canRead(key);
   }

   @Override
   protected Object setSkipRemoteGetsAndInvokeNextForDataCommand(InvocationContext ctx, DataWriteCommand command) {
      return invokeNextAndHandle(ctx, command, handleDataWriteReturn);
   }

   @Override
   protected Object setSkipRemoteGetsAndInvokeNextForManyEntriesCommand(InvocationContext ctx, WriteCommand command) {
      return invokeNextAndHandle(ctx, command, handleManyWriteReturn);
   }

   private Object handleDataWriteReturn(InvocationContext ctx, VisitableCommand command, Object rv, Throwable throwable) throws Throwable {
      if (throwable != null) {
         return super.handleDataWriteReturn(ctx, command, throwable);
      } else if (command.isSuccessful() && ctx.isOriginLocal()) {
         DataWriteCommand dataWriteCommand = (DataWriteCommand) command;
         if (dataWriteCommand.hasAnyFlag(NOT_BIASING_FLAGS)) {
            return rv;
         }
         if (!distributionManager.getCacheTopology().getDistribution(dataWriteCommand.getKey()).isPrimary()) {
            biasManager.addLocalBias(dataWriteCommand.getKey(), dataWriteCommand.getTopologyId());
         }
      }
      return rv;
   }

   private Object handleManyWriteReturn(InvocationContext ctx, VisitableCommand command, Object rv, Throwable throwable) throws Throwable {
      if (throwable != null) {
         return super.handleManyWriteReturn(ctx, command, throwable);
      } else if (command.isSuccessful() && ctx.isOriginLocal()) {
         WriteCommand writeCommand = (WriteCommand) command;
         if (writeCommand.hasAnyFlag(NOT_BIASING_FLAGS)) {
            return rv;
         }
         for (Object key : writeCommand.getAffectedKeys()) {
            if (!distributionManager.getCacheTopology().getDistribution(key).isPrimary()) {
               biasManager.addLocalBias(key, writeCommand.getTopologyId());
            }
         }
      }
      return rv;
   }
}
