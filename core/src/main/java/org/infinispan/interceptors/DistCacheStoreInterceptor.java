package org.infinispan.interceptors;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Map;

/**
 * Cache store interceptor specific for the distribution and replication cache modes.
 *
 * <p>If the cache store is shared, only the primary owner of the key writes to the cache store.</p>
 * <p>If the cache store is not shared, every owner of a key writes to the cache store.</p>
 * <p>In non-tx caches, if the originator is an owner, the command is executed there twice. The first time,
 * ({@code isOriginLocal() == true}) we don't write anything to the cache store; the second time,
 * the normal rules apply.</p>
 * <p>For clear operations, either only the originator of the command clears the cache store (if it is
 * shared), or every node clears its cache store (if it is not shared). Note that in non-tx caches, this
 * happens without holding a lock on the primary owner of all the keys.</p>
 *
 * @author Galder Zamarreño
 * @author Dan Berindei
 * @since 4.0
 */
public class DistCacheStoreInterceptor extends CacheStoreInterceptor {
   private DistributionManager dm;
   private Transport transport;
   private Address address;

   private static final Log log = LogFactory.getLog(DistCacheStoreInterceptor.class);
   private boolean isUsingLockDelegation;
   private ClusteringDependentLogic cdl;

   @Override
   protected Log getLog() {
      return log;
   }

   @Inject
   public void inject(DistributionManager dm, Transport transport, ClusteringDependentLogic cdl) {
      this.dm = dm;
      this.transport = transport;
      this.cdl = cdl;
   }

   @Start(priority = 25) // after the distribution manager!
   @SuppressWarnings("unused")
   private void setAddress() {
      this.address = transport.getAddress();
      this.isUsingLockDelegation = !cacheConfiguration.transaction().transactionMode().isTransactional();
   }

   // ---- WRITE commands

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      Object returnValue = invokeNextInterceptor(ctx, command);
      Object key = command.getKey();
      if (!isStoreEnabled(command) || ctx.isInTxScope() || !command.isSuccessful()) return returnValue;
      if (!isProperWriter(ctx, command, command.getKey())) return returnValue;

      InternalCacheEntry se = getStoredEntry(key, ctx);
      store.store(se);
      log.tracef("Stored entry %s under key %s", se, key);
      if (getStatisticsEnabled()) cacheStores.incrementAndGet();
      return returnValue;
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      Object returnValue = invokeNextInterceptor(ctx, command);
      if (!isStoreEnabled(command) || ctx.isInTxScope()) return returnValue;

      Map<Object, Object> map = command.getMap();
      int count = 0;
      for (Object key : map.keySet()) {
         // In non-tx mode, a node may receive the same forwarded PutMapCommand many times - but each time
         // it must write only the keys locked on the primary owner that forwarded the command
         if (isUsingLockDelegation && command.isForwarded() && !dm.getPrimaryLocation(key).equals(ctx.getOrigin()))
            continue;

         if (isProperWriter(ctx, command, key)) {
            InternalCacheEntry se = getStoredEntry(key, ctx);
            store.store(se);
            log.tracef("Stored entry %s under key %s", se, key);
            count++;
         }
      }
      if (getStatisticsEnabled()) cacheStores.getAndAdd(count);
      return returnValue;
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      Object retval = invokeNextInterceptor(ctx, command);
      Object key = command.getKey();
      if (!isStoreEnabled(command) || ctx.isInTxScope() || !command.isSuccessful()) return retval;
      if (!isProperWriter(ctx, command, key)) return retval;

      boolean resp = store.remove(key);
      log.tracef("Removed entry under key %s and got response %s from CacheStore", key, resp);
      return retval;
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command)
         throws Throwable {
      Object returnValue = invokeNextInterceptor(ctx, command);
      Object key = command.getKey();
      if (!isStoreEnabled(command) || ctx.isInTxScope() || !command.isSuccessful()) return returnValue;
      if (!isProperWriter(ctx, command, command.getKey())) return returnValue;

      InternalCacheEntry se = getStoredEntry(key, ctx);
      store.store(se);
      log.tracef("Stored entry %s under key %s", se, key);
      if (getStatisticsEnabled()) cacheStores.incrementAndGet();

      return returnValue;
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      if (isStoreEnabled()) {
         if (getLog().isTraceEnabled()) getLog().trace("Transactional so don't put stuff in the cache store yet.");
         prepareCacheLoader(ctx, command.getGlobalTransaction(), ctx, command.isOnePhaseCommit());
      }
      return invokeNextInterceptor(ctx, command);
   }

   @Override
   public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      if (isStoreEnabled(command) && !ctx.isInTxScope() && isProperWriterForClear(ctx)) {
         clearCacheStore();
      }

      return invokeNextInterceptor(ctx, command);
   }

   @Override
   protected boolean isProperWriter(InvocationContext ctx, FlagAffectedCommand command, Object key) {
      if (command.hasFlag(Flag.SKIP_OWNERSHIP_CHECK))
         return true;

      if (loaderConfig.shared()) {
         if (!dm.getPrimaryLocation(key).equals(address)) {
            log.tracef("Skipping cache store since the cache loader is shared " +
                  "and the caller is not the first owner of the key %s", key);
            return false;
         }
      } else {
         if (isUsingLockDelegation && !command.hasFlag(Flag.CACHE_MODE_LOCAL)) {
            if (ctx.isOriginLocal() && !dm.getPrimaryLocation(key).equals(address)) {
               // The command will be forwarded back to the originator, and the value will be stored then
               // (while holding the lock on the primary owner).
               log.tracef("Skipping cache store on the originator because it is not the primary owner " +
                     "of key %s", key);
               return false;
            }
         }
         if (!dm.getWriteConsistentHash().isKeyLocalToNode(address, key)) {
            log.tracef("Skipping cache store since the key is not local: %s", key);
            return false;
         }
      }
      return true;
   }

   protected boolean isProperWriterForClear(InvocationContext ctx) {
      // Note: In non-tx mode, the originator doesn't acquire any locks - so other nodes may write
      // to the cache store while we are clearing it.
      return !loaderConfig.shared() || ctx.isOriginLocal();
   }

}
