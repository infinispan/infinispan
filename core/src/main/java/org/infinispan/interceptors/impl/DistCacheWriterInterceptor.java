package org.infinispan.interceptors.impl;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.infinispan.commons.util.Util.toStr;
import static org.infinispan.persistence.manager.PersistenceManager.AccessMode.BOTH;
import static org.infinispan.persistence.manager.PersistenceManager.AccessMode.PRIVATE;

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
 * @since 9.0
 */
public class DistCacheWriterInterceptor extends CacheWriterInterceptor {
   private DistributionManager dm;
   private Transport transport;
   private Address address;

   private static final Log log = LogFactory.getLog(DistCacheWriterInterceptor.class);
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
   public CompletableFuture<Void> visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      Object returnValue = ctx.forkInvocationSync(command);
      Object key = command.getKey();
      if (!isStoreEnabled(command) || ctx.isInTxScope() || !command.isSuccessful()) return ctx.shortCircuit(returnValue);
      if (!isProperWriter(ctx, command, command.getKey())) return ctx.shortCircuit(returnValue);

      storeEntry(ctx, key, command);
      if (getStatisticsEnabled()) cacheStores.incrementAndGet();
      return ctx.shortCircuit(returnValue);
   }

   @Override
   public CompletableFuture<Void> visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      Object returnValue = ctx.forkInvocationSync(command);
      if (!isStoreEnabled(command) || ctx.isInTxScope()) return ctx.shortCircuit(returnValue);

      Map<Object, Object> map = command.getMap();
      int count = 0;
      for (Object key : map.keySet()) {
         // In non-tx mode, a node may receive the same forwarded PutMapCommand many times - but each time
         // it must write only the keys locked on the primary owner that forwarded the command
         if (isUsingLockDelegation && command.isForwarded() && !dm.getPrimaryLocation(key).equals(ctx.getOrigin()))
            continue;

         if (isProperWriter(ctx, command, key)) {
            storeEntry(ctx, key, command);
            count++;
         }
      }
      if (getStatisticsEnabled()) cacheStores.getAndAdd(count);
      return ctx.shortCircuit(returnValue);
   }

   @Override
   public CompletableFuture<Void> visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      Object retval = ctx.forkInvocationSync(command);
      Object key = command.getKey();
      if (!isStoreEnabled(command) || ctx.isInTxScope() || !command.isSuccessful()) return ctx.shortCircuit(retval);
      if (!isProperWriter(ctx, command, key)) return ctx.shortCircuit(retval);

      boolean resp = persistenceManager.deleteFromAllStores(key, skipSharedStores(ctx, key, command) ? PRIVATE : BOTH);
      log.tracef("Removed entry under key %s and got response %s from CacheStore", key, resp);
      return ctx.shortCircuit(retval);
   }

   @Override
   public CompletableFuture<Void> visitReplaceCommand(InvocationContext ctx, ReplaceCommand command)
         throws Throwable {
      Object returnValue = ctx.forkInvocationSync(command);
      Object key = command.getKey();
      if (!isStoreEnabled(command) || ctx.isInTxScope() || !command.isSuccessful()) return ctx.shortCircuit(returnValue);
      if (!isProperWriter(ctx, command, command.getKey())) return ctx.shortCircuit(returnValue);

      storeEntry(ctx, key, command);
      if (getStatisticsEnabled()) cacheStores.incrementAndGet();

      return ctx.shortCircuit(returnValue);
   }

   @Override
   protected boolean skipSharedStores(InvocationContext ctx, Object key, FlagAffectedCommand command) {
      return !cdl.localNodeIsPrimaryOwner(key) || command.hasFlag(Flag.SKIP_SHARED_CACHE_STORE);
   }

   @Override
   protected boolean isProperWriter(InvocationContext ctx, FlagAffectedCommand command, Object key) {
      if (command.hasFlag(Flag.SKIP_OWNERSHIP_CHECK))
         return true;

      if (isUsingLockDelegation && !command.hasFlag(Flag.CACHE_MODE_LOCAL)) {
         if (ctx.isOriginLocal() && !dm.getPrimaryLocation(key).equals(address)) {
            // The command will be forwarded back to the originator, and the value will be stored then
            // (while holding the lock on the primary owner).
            log.tracef("Skipping cache store on the originator because it is not the primary owner " +
                             "of key %s", toStr(key));
            return false;
         }
      }
      if (!dm.getWriteConsistentHash().isKeyLocalToNode(address, key)) {
         log.tracef("Skipping cache store since the key is not local: %s", key);
         return false;
      }
      return true;
   }
}
