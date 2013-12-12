package org.infinispan.interceptors.distribution;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.jgroups.SuspectException;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.*;

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
      try {
         Object returnValue = invokeNextInterceptor(ctx, command);
         if (returnValue == null) {
            Object key = command.getKey();
            if (needsRemoteGet(ctx, command)) {
               InternalCacheEntry remoteEntry = remoteGetCacheEntry(ctx, key, command);
               returnValue = computeGetReturn(remoteEntry, command);
            }
            if (returnValue == null && isValueAvailableLocally(dm.getReadConsistentHash(), key)) {
               InternalCacheEntry localEntry = localGetCacheEntry(ctx, key, false, command);
               returnValue = computeGetReturn(localEntry, command);
            }
         }
         return returnValue;
      } catch (SuspectException e) {
         // retry
         return visitGetKeyValueCommand(ctx, command);
      }
   }

   private Object computeGetReturn(InternalCacheEntry entry, GetKeyValueCommand command) {
      if (!command.isReturnEntry() && entry != null)
         return entry.getValue();

      return entry;
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      return handleNonTxWriteCommand(ctx, command);
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      if (ctx.isOriginLocal()) {
         Set<Address> primaryOwners = new HashSet<Address>(command.getAffectedKeys().size());
         for (Object k : command.getAffectedKeys()) {
            primaryOwners.add(cdl.getPrimaryOwner(k));
         }
         primaryOwners.remove(rpcManager.getAddress());
         if (!primaryOwners.isEmpty()) {
            rpcManager.invokeRemotely(primaryOwners, command, rpcManager.getDefaultRpcOptions(isSynchronous(command)));
         }
      }

      if (!command.isForwarded()) {
         //I need to forward this to all the nodes that are secondary owners
         Set<Object> keysIOwn = new HashSet<Object>(command.getAffectedKeys().size());
         for (Object k : command.getAffectedKeys()) {
            if (cdl.localNodeIsPrimaryOwner(k)) {
               keysIOwn.add(k);
            }
         }
         Collection<Address> backupOwners = cdl.getOwners(keysIOwn);
         if (backupOwners == null || !backupOwners.isEmpty()) {
            command.setFlags(Flag.SKIP_LOCKING);
            command.setForwarded(true);
            rpcManager.invokeRemotely(backupOwners, command, rpcManager.getDefaultRpcOptions(isSynchronous(command)));
            command.setForwarded(false);
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

   /**
    * Don't forward in the case of clear commands, just acquire local locks and broadcast.
    */
   @Override
   public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      if (ctx.isOriginLocal() && !isLocalModeForced(command)) {
         rpcManager.invokeRemotely(null, command, rpcManager.getDefaultRpcOptions(isSynchronous(command)));
      }
      return invokeNextInterceptor(ctx, command);
   }

   protected void remoteGetBeforeWrite(InvocationContext ctx, WriteCommand command, RecipientGenerator keygen) throws Throwable {
      // this should only happen if:
      //   a) unsafeUnreliableReturnValues is false
      //   b) unsafeUnreliableReturnValues is true, the command is conditional or a delta write
      // On the backup owners, the value matching policy should be set to MATCH_ALWAYS, and command.isConditional() should return true
      if (isNeedReliableReturnValues(command) || command.isConditional() || command.hasFlag(Flag.DELTA_WRITE)) {
         for (Object k : keygen.getKeys()) {
            if (cdl.localNodeIsPrimaryOwner(k)) {
               // Then it makes sense to try a local get and wrap again. This will compensate the fact the the entry was not local
               // earlier when the EntryWrappingInterceptor executed during current invocation context but it should be now.
               localGetCacheEntry(ctx, k, true, command);
            }
         }
      }
   }

   private InternalCacheEntry localGetCacheEntry(InvocationContext ctx, Object key, boolean isWrite, FlagAffectedCommand command) throws Throwable {
      InternalCacheEntry ice = dataContainer.get(key);
      if (ice != null) {
         if (!ctx.replaceValue(key, ice))  {
            if (isWrite)
               entryFactory.wrapEntryForPut(ctx, key, ice, false, command, true);
            else
               ctx.putLookedUpEntry(key, ice);
         }
         return ice;
      }
      return null;
   }

   private InternalCacheEntry remoteGetCacheEntry(InvocationContext ctx, Object key, GetKeyValueCommand command) throws Throwable {
      if (trace) log.tracef("Doing a remote get for key %s", key);
      InternalCacheEntry ice = retrieveFromRemoteSource(key, ctx, false, command, false);
      command.setRemotelyFetchedValue(ice);
      if (ice != null)
         return ice;

      return null;
   }
}
