package org.infinispan.xsite;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.Cache;
import org.infinispan.remoting.LocalInvocation;
import org.infinispan.util.concurrent.CompletionStages;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.commands.XSiteStateTransferFinishReceiveCommand;
import org.infinispan.xsite.commands.XSiteStateTransferStartReceiveCommand;
import org.infinispan.xsite.statetransfer.XSiteState;
import org.infinispan.xsite.statetransfer.XSiteStatePushCommand;

/**
 * {@link org.infinispan.xsite.BackupReceiver} implementation for local caches.
 *
 * @author Pedro Ruivo
 * @since 7.1
 */
public class LocalCacheBackupReceiver extends BaseBackupReceiver {

   private static final Log log = LogFactory.getLog(LocalCacheBackupReceiver.class);
   private static final boolean trace = log.isTraceEnabled();

   LocalCacheBackupReceiver(Cache<Object, Object> cache) {
      super(cache);
   }

   @Override
   public CompletionStage<Void> handleStartReceivingStateTransfer(XSiteStateTransferStartReceiveCommand command) {
      command = XSiteStateTransferStartReceiveCommand.copyForCache(command, cacheName);
      return CompletionStages.ignoreValue(LocalInvocation.newInstanceFromCache(cache, command).callAsync());
   }

   @Override
   public CompletionStage<Void> handleEndReceivingStateTransfer(XSiteStateTransferFinishReceiveCommand command) {
      command = XSiteStateTransferFinishReceiveCommand.copyForCache(command, cacheName);
      return CompletionStages.ignoreValue(LocalInvocation.newInstanceFromCache(cache, command).callAsync());
   }

   @Override
   public CompletionStage<Void> handleStateTransferState(XSiteStatePushCommand cmd) {
      //split the state and forward it to the primary owners...
      CompletableFuture<Void> allowInvocation = checkInvocationAllowedFuture();
      if (allowInvocation != null) {
         return allowInvocation;
      }

      final List<XSiteState> localChunks = Arrays.asList(cmd.getChunk());

      if (trace) {
         log.tracef("Local node will apply %s", localChunks);
      }

      return LocalInvocation.newInstanceFromCache(cache, newStatePushCommand(cache, localChunks)).callAsync()
            .thenApply(this::assertAllowInvocationFunction);
   }

}
