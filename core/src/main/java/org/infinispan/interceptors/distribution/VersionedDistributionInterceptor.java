package org.infinispan.interceptors.distribution;

import static org.infinispan.transaction.impl.WriteSkewHelper.readVersionsFromResponse;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.InequalVersionComparisonResult;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.metadata.Metadata;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.impl.MapResponseCollector;
import org.infinispan.transaction.impl.AbstractCacheTransaction;
import org.infinispan.transaction.impl.LocalTransaction;
import org.infinispan.transaction.xa.CacheTransaction;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * A version of the {@link TxDistributionInterceptor} that adds logic to handling prepares when entries are
 * versioned.
 *
 * @author Manik Surtani
 * @author Dan Berindei
 */
public class VersionedDistributionInterceptor extends TxDistributionInterceptor {

   private static final Log log = LogFactory.getLog(VersionedDistributionInterceptor.class);

   @Inject private VersionGenerator versionGenerator;

   @Override
   protected Log getLog() {
      return log;
   }

   @Override
   protected void wrapRemoteEntry(InvocationContext ctx, Object key, CacheEntry ice, boolean isWrite) {
      if (ctx.isInTxScope()) {
         AbstractCacheTransaction cacheTransaction = ((TxInvocationContext) ctx).getCacheTransaction();
         EntryVersion seenVersion = cacheTransaction.getVersionsRead().get(key);
         if (seenVersion != null) {
            EntryVersion newVersion = null;
            if (ice != null && ice.getMetadata() != null) {
               newVersion = ice.getMetadata().version();
            }
            if (newVersion == null) {
               throw new IllegalStateException("Wrapping entry without version");
            }
            if (seenVersion.compareTo(newVersion) != InequalVersionComparisonResult.EQUAL) {
               if (ctx.lookupEntry(key) == null) {
                  // We have read the entry using functional command on remote node and now we want
                  // the full entry, but we cannot provide the same version as the already read one.
                  throw log.writeSkewOnRead(key, key, seenVersion, newVersion);
               } else {
                  // We have retrieved remote entry despite being already wrapped: that can happen
                  // for GetKeysInGroupCommand which does not know what entries will it fetch.
                  // We can safely ignore the newly fetched value.
                  return;
               }
            }
         }
      }
      super.wrapRemoteEntry(ctx, key, ice, isWrite);
   }

   @Override
   protected Object wrapFunctionalResultOnNonOriginOnReturn(Object rv, CacheEntry entry) {
      Metadata metadata = entry.getMetadata();
      EntryVersion version = metadata == null || metadata.version() == null ?
            versionGenerator.nonExistingVersion() : metadata.version();
      return new VersionedResult(rv, version);
   }

   @Override
   protected Object wrapFunctionalManyResultOnNonOrigin(InvocationContext ctx, Collection<?> keys, Object[] values) {
      // note: this relies on the fact that keys are already ordered on remote node
      EntryVersion[] versions = new EntryVersion[keys.size()];
      int i = 0;
      for (Object key : keys) {
         CacheEntry entry = ctx.lookupEntry(key);
         Metadata metadata = entry.getMetadata();
         versions[i++] = metadata == null || metadata.version() == null ?
               versionGenerator.nonExistingVersion() : metadata.version();
      }
      return new VersionedResults(values, versions);
   }

   @Override
   protected Object[] unwrapFunctionalManyResultOnOrigin(InvocationContext ctx, List<Object> keys, Object responseValue) {
      if (responseValue instanceof VersionedResults) {
         VersionedResults vrs = (VersionedResults) responseValue;
         if (ctx.isInTxScope()) {
            AbstractCacheTransaction tx = ((TxInvocationContext) ctx).getCacheTransaction();
            for (int i = 0; i < vrs.versions.length; ++i) {
               checkAndAddReadVersion(tx, keys.get(i), vrs.versions[i]);
            }
         }
         return vrs.values;
      } else {
         return null;
      }
   }

   @Override
   protected Object unwrapFunctionalResultOnOrigin(InvocationContext ctx, Object key, Object responseValue) {
      VersionedResult vr = (VersionedResult) responseValue;
      // As an optimization, read-only single-key commands are executed in SingleKeyNonTxInvocationContext
      if (ctx.isInTxScope()) {
         AbstractCacheTransaction tx = ((TxInvocationContext) ctx).getCacheTransaction();
         checkAndAddReadVersion(tx, key, vr.version);
      }
      return vr.result;
   }

   private void checkAndAddReadVersion(AbstractCacheTransaction tx, Object key, EntryVersion version) {
      EntryVersion lastVersion = tx.getLookedUpRemoteVersion(key);
      // TODO: should we check the write skew configuration here?
      // TODO: version seen or looked up remote version?
      if (lastVersion != null && lastVersion.compareTo(version) != InequalVersionComparisonResult.EQUAL) {
         throw log.writeSkewOnRead(key, key, lastVersion, version);
      }
      EntryVersion lastVersionSeen = tx.getVersionsRead().get(key);
      if (lastVersionSeen != null && lastVersionSeen.compareTo(version) != InequalVersionComparisonResult.EQUAL) {
         throw log.writeSkewOnRead(key, key, lastVersionSeen, version);
      }
      tx.addVersionRead(key, version);
   }

   @Override
   protected CompletionStage<Object> prepareOnAffectedNodes(TxInvocationContext<?> ctx, PrepareCommand command, Collection<Address> recipients) {
      CompletionStage<Map<Address, Response>> remoteInvocation;
      if (recipients != null) {
         MapResponseCollector collector = MapResponseCollector.ignoreLeavers(recipients.size());
         remoteInvocation = rpcManager.invokeCommand(recipients, command, collector, rpcManager.getSyncRpcOptions());
      } else {
         MapResponseCollector collector = MapResponseCollector.ignoreLeavers();
         remoteInvocation = rpcManager.invokeCommandOnAll(command, collector, rpcManager.getSyncRpcOptions());
      }
      return remoteInvocation.handle((responses, t) -> {
         transactionRemotelyPrepared(ctx);
         CompletableFutures.rethrowException(t);

         checkTxCommandResponses(responses, command, (TxInvocationContext<LocalTransaction>) ctx, recipients);

         // Now store newly generated versions from lock owners for use during the commit phase.
         CacheTransaction ct = ctx.getCacheTransaction();
         for (Response r : responses.values()) readVersionsFromResponse(r, ct);
         return null;
      });
   }
}
