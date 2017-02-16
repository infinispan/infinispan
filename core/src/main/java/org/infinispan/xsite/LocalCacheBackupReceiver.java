package org.infinispan.xsite;

import java.util.Arrays;
import java.util.List;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
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

   public LocalCacheBackupReceiver(Cache<Object, Object> cache) {
      super(cache);
   }

   @Override
   public void handleStateTransferControl(XSiteStateTransferControlCommand command) throws Exception {
      XSiteStateTransferControlCommand invokeCommand = command;
      if (!command.getCacheName().equals(cacheName)) {
         //copy if the cache name is different
         invokeCommand = command.copyForCache(cacheName);
      }
      invokeCommand.setSiteName(command.getOriginSite());
      LocalInvocation.newInstanceFromCache(cache, invokeCommand).call();
   }

   @Override
   public void handleStateTransferState(XSiteStatePushCommand cmd) throws Exception {
      //split the state and forward it to the primary owners...
      assertAllowInvocation();

      final List<XSiteState> localChunks = Arrays.asList(cmd.getChunk());

      if (trace) {
         log.tracef("Local node will apply %s", localChunks);
      }

      LocalInvocation.newInstanceFromCache(cache, newStatePushCommand(cache, localChunks)).call();

      //the put operation can fail silently. check in the end and it is better to resend the chunk than to lose keys.
      if (!cache.getStatus().allowInvocations()) {
         throw new CacheException("Cache is stopping or terminated: " + cache.getStatus());
      }
   }

}
