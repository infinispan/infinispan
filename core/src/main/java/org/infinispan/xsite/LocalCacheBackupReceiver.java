package org.infinispan.xsite;

import static org.infinispan.util.concurrent.CompletableFutures.toNullFunction;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.Cache;
import org.infinispan.remoting.LocalInvocation;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.statetransfer.XSiteState;
import org.infinispan.xsite.statetransfer.XSiteStatePushCommand;
import org.infinispan.xsite.statetransfer.XSiteStateTransferControlCommand;

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
   public CompletionStage<Void> handleStateTransferControl(XSiteStateTransferControlCommand command) {
      XSiteStateTransferControlCommand invokeCommand = command;
      if (!command.getCacheName().equals(cacheName)) {
         //copy if the cache name is different
         invokeCommand = command.copyForCache(cacheName);
      }
      invokeCommand.setSiteName(command.getOriginSite());
      return LocalInvocation.newInstanceFromCache(cache, invokeCommand).callAsync().thenApply(toNullFunction());
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
