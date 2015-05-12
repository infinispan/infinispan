package org.infinispan.interceptors.distribution;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.read.AbstractDataCommand;
import org.infinispan.commands.read.GetCacheEntryCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.read.RemoteFetchingCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.util.concurrent.CompositeNotifyingFuture;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.util.ReadOnlySegmentAwareMap;
import org.infinispan.remoting.RemoteException;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Non-transactional interceptor used by distributed caches that support concurrent writes.
 * It is implemented based on lock forwarding. E.g.
 * - 'k' is written on node A, owners(k)={B,C}
 * - A forwards the given command to B
 * - B acquires a lock on 'k' then it forwards it to the remaining owners: C
 * - C applies the change and returns to B (no lock acquisition is needed)
 * - B applies the result as well, releases the lock and returns the result of the operation to A.
 * <p/>
 * Note that even though this introduces an additional RPC (the forwarding), it behaves very well in conjunction with
 * consistent-hash aware hotrod clients which connect directly to the lock owner.
 *
 * @author Mircea Markus
 * @since 5.2
 */
public class NonTxDistributionInterceptor extends BaseDistributionInterceptor {

   private static Log log = LogFactory.getLog(NonTxDistributionInterceptor.class);
   private static final boolean trace = log.isTraceEnabled();

   @Override
   public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      return visitRemoteFetchingCommand(ctx, command, false);
   }

   @Override
   public Object visitGetCacheEntryCommand(InvocationContext ctx, GetCacheEntryCommand command) throws Throwable {
      return visitRemoteFetchingCommand(ctx, command, true);
   }

   private <T extends AbstractDataCommand & RemoteFetchingCommand> Object visitRemoteFetchingCommand(InvocationContext ctx, T command, boolean returnEntry) throws Throwable {
      Object returnValue = invokeNextInterceptor(ctx, command);
      if (returnValue == null) {
         Object key = command.getKey();
         if (needsRemoteGet(ctx, command)) {
            InternalCacheEntry remoteEntry = remoteGetCacheEntry(ctx, key, command);
            returnValue = computeGetReturn(remoteEntry, returnEntry);
         }
         if (returnValue == null) {
            InternalCacheEntry localEntry = fetchValueLocallyIfAvailable(dm.getReadConsistentHash(), key);
            if (localEntry != null) {
               wrapInternalCacheEntry(localEntry, ctx, key, false, command);
            }
            returnValue = computeGetReturn(localEntry, returnEntry);
         }
      }
      return returnValue;
   }

   private Object computeGetReturn(InternalCacheEntry entry, boolean returnEntry) {
      if (!returnEntry && entry != null)
         return entry.getValue();
      return entry;
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      return handleNonTxWriteCommand(ctx, command);
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      Map<Object, Object> originalMap = command.getMap();
      ConsistentHash ch = dm.getConsistentHash();
      Address localAddress = rpcManager.getAddress();
      if (ctx.isOriginLocal()) {
         List<CompletableFuture<Map<Address, Response>>> futures = new ArrayList<>(
               rpcManager.getMembers().size() - 1);
         // TODO: if async we don't need to do futures...
         RpcOptions options = rpcManager.getDefaultRpcOptions(isSynchronous(command));
         for (Address member : rpcManager.getMembers()) {
            if (member.equals(rpcManager.getAddress())) {
               continue;
            }
            Set<Integer> segments = ch.getPrimarySegmentsForOwner(member);
            if (!segments.isEmpty()) {
               Map<Object, Object> segmentEntriesMap =
                     new ReadOnlySegmentAwareMap<>(originalMap, ch, segments);
               if (!segmentEntriesMap.isEmpty()) {
                  PutMapCommand copy = new PutMapCommand(command);
                  copy.setMap(segmentEntriesMap);
                  CompletableFuture<Map<Address, Response>> future = rpcManager.invokeRemotelyAsync(
                        Collections.singletonList(member), copy, options);
                  futures.add(future);
               }
            }
         }
         if (futures.size() > 0) {
            CompletableFuture[] futuresArray = new CompletableFuture[futures.size()];
            CompletableFuture<Void> compFuture = CompletableFuture.allOf(futures.toArray(futuresArray));
            try {
               compFuture.get(options.timeout(), TimeUnit.MILLISECONDS);
            } catch (ExecutionException e) {
               throw new RemoteException("Exception while processing put on primary owner", e.getCause());
            } catch (TimeoutException e) {
               throw new CacheException(e);
            }
         }
      }

      if (!command.isForwarded() && ch.getNumOwners() > 1) {
         // Now we find all the segments that we own and map our backups to those
         Map<Address, Set<Integer>> backupOwnerSegments = new HashMap<>();
         int segmentCount = ch.getNumSegments();
         for (int i = 0; i < segmentCount; ++i) {
            Iterator<Address> iter = ch.locateOwnersForSegment(i).iterator();

            if (iter.next().equals(localAddress)) {
               while (iter.hasNext()) {
                  Address backupOwner = iter.next();
                  Set<Integer> segments = backupOwnerSegments.get(backupOwner);
                  if (segments == null) {
                     backupOwnerSegments.put(backupOwner, (segments = new HashSet<>()));
                  }
                  segments.add(i);
               }
            }
         }

         int backupOwnerSize = backupOwnerSegments.size();
         if (backupOwnerSize > 0) {
            List<CompletableFuture<Map<Address, Response>>> futures = new ArrayList<>(backupOwnerSize);
            RpcOptions options = rpcManager.getDefaultRpcOptions(isSynchronous(command));
            command.setFlags(Flag.SKIP_LOCKING);
            command.setForwarded(true);

            for (Entry<Address, Set<Integer>> entry : backupOwnerSegments.entrySet()) {
               Set<Integer> segments = entry.getValue();
               Map<Object, Object> segmentEntriesMap = 
                     new ReadOnlySegmentAwareMap<>(originalMap, ch, segments);
               if (!segmentEntriesMap.isEmpty()) {
                  PutMapCommand copy = new PutMapCommand(command);
                  copy.setMap(segmentEntriesMap);
                  CompletableFuture<Map<Address, Response>> future = rpcManager.invokeRemotelyAsync(
                        Collections.singletonList(entry.getKey()), copy, options);
                  futures.add(future);
               }
            }
            command.setForwarded(false);
            if (futures.size() > 0) {
               CompletableFuture[] futuresArray = new CompletableFuture[futures.size()];
               CompletableFuture<Void> compFuture = CompletableFuture.allOf(futures.toArray(futuresArray));
               try {
                  compFuture.get(options.timeout(), TimeUnit.MILLISECONDS);
               } catch (ExecutionException e) {
                  throw new RemoteException("Exception while processing put on backup owner", e.getCause());
               } catch (TimeoutException e) {
                  throw new CacheException(e);
               }
            }
         }
      }

      return invokeNextInterceptor(ctx, command);
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      return handleNonTxWriteCommand(ctx, command);
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      return handleNonTxWriteCommand(ctx, command);
   }

   protected void remoteGetBeforeWrite(InvocationContext ctx, WriteCommand command, Object key) throws Throwable {
      if (cdl.localNodeIsPrimaryOwner(key)) {
         // Then it makes sense to try a local get and wrap again. This will compensate the fact the the entry was not local
         // earlier when the EntryWrappingInterceptor executed during current invocation context but it should be now.
         localGetCacheEntry(ctx, key, true, command);
      }
   }

   private InternalCacheEntry localGetCacheEntry(InvocationContext ctx, Object key, boolean isWrite, FlagAffectedCommand command) throws Throwable {
      InternalCacheEntry ice = dataContainer.get(key);
      if (ice != null) {
         wrapInternalCacheEntry(ice, ctx, key, isWrite, command);
         return ice;
      }
      return null;
   }

   private void wrapInternalCacheEntry(InternalCacheEntry ice, InvocationContext ctx, Object key, boolean isWrite,
                                       FlagAffectedCommand command) {
      if (!ctx.replaceValue(key, ice))  {
         if (isWrite)
            entryFactory.wrapEntryForPut(ctx, key, ice, false, command, true);
         else
            ctx.putLookedUpEntry(key, ice);
      }
   }

   private <T extends FlagAffectedCommand & RemoteFetchingCommand> InternalCacheEntry remoteGetCacheEntry(InvocationContext ctx, Object key, T command) throws Throwable {
      if (trace) log.tracef("Doing a remote get for key %s", key);
      InternalCacheEntry ice = retrieveFromRemoteSource(key, ctx, false, command, false);
      command.setRemotelyFetchedValue(ice);
      return ice;
   }

   protected boolean needValuesFromPreviousOwners(InvocationContext ctx, WriteCommand command) {
      if (command.hasFlag(Flag.PUT_FOR_STATE_TRANSFER)) return false;
      if (command.hasFlag(Flag.DELTA_WRITE) && !command.hasFlag(Flag.CACHE_MODE_LOCAL)) return true;

      // The return value only matters on the primary owner.
      // The conditional commands also check the previous value only on the primary owner.
      // Note: This should not be necessary, as the primary owner always has the previous value
      if (isNeedReliableReturnValues(command) || command.isConditional()) {
         for (Object key : command.getAffectedKeys()) {
            if (cdl.localNodeIsPrimaryOwner(key)) return true;
         }
      }
      return false;
   }
}
