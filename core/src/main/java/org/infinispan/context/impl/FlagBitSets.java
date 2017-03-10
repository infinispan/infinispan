package org.infinispan.context.impl;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;

/**
 * Pre-computed bitsets containing each flag.
 *
 * @author Dan Berindei
 * @since 9.0
 */
public class FlagBitSets {
   public static final long ZERO_LOCK_ACQUISITION_TIMEOUT = EnumUtil.bitSetOf(Flag.ZERO_LOCK_ACQUISITION_TIMEOUT);
   public static final long CACHE_MODE_LOCAL = EnumUtil.bitSetOf(Flag.CACHE_MODE_LOCAL);
   public static final long SKIP_LOCKING = EnumUtil.bitSetOf(Flag.SKIP_LOCKING);
   public static final long FORCE_WRITE_LOCK = EnumUtil.bitSetOf(Flag.FORCE_WRITE_LOCK);
   public static final long FORCE_ASYNCHRONOUS = EnumUtil.bitSetOf(Flag.FORCE_ASYNCHRONOUS);
   public static final long FORCE_SYNCHRONOUS = EnumUtil.bitSetOf(Flag.FORCE_SYNCHRONOUS);
   public static final long SKIP_CACHE_STORE = EnumUtil.bitSetOf(Flag.SKIP_CACHE_STORE);
   public static final long SKIP_CACHE_LOAD = EnumUtil.bitSetOf(Flag.SKIP_CACHE_LOAD);
   public static final long FAIL_SILENTLY = EnumUtil.bitSetOf(Flag.FAIL_SILENTLY);
   public static final long SKIP_REMOTE_LOOKUP = EnumUtil.bitSetOf(Flag.SKIP_REMOTE_LOOKUP);
   public static final long SKIP_INDEXING = EnumUtil.bitSetOf(Flag.SKIP_INDEXING);
   public static final long PUT_FOR_EXTERNAL_READ = EnumUtil.bitSetOf(Flag.PUT_FOR_EXTERNAL_READ);
   public static final long PUT_FOR_STATE_TRANSFER = EnumUtil.bitSetOf(Flag.PUT_FOR_STATE_TRANSFER);
   public static final long PUT_FOR_X_SITE_STATE_TRANSFER = EnumUtil.bitSetOf(Flag.PUT_FOR_X_SITE_STATE_TRANSFER);
   public static final long SKIP_SHARED_CACHE_STORE = EnumUtil.bitSetOf(Flag.SKIP_SHARED_CACHE_STORE);
   @Deprecated
   public static final long REMOVE_DATA_ON_STOP = EnumUtil.bitSetOf(Flag.REMOVE_DATA_ON_STOP);
   public static final long SKIP_OWNERSHIP_CHECK = EnumUtil.bitSetOf(Flag.SKIP_OWNERSHIP_CHECK);
   @Deprecated
   public static final long DELTA_WRITE = EnumUtil.bitSetOf(Flag.DELTA_WRITE);
   public static final long IGNORE_RETURN_VALUES = EnumUtil.bitSetOf(Flag.IGNORE_RETURN_VALUES);
   public static final long SKIP_XSITE_BACKUP = EnumUtil.bitSetOf(Flag.SKIP_XSITE_BACKUP);
   public static final long GUARANTEED_DELIVERY = EnumUtil.bitSetOf(Flag.GUARANTEED_DELIVERY);
   public static final long SKIP_LISTENER_NOTIFICATION = EnumUtil.bitSetOf(Flag.SKIP_LISTENER_NOTIFICATION);
   public static final long SKIP_STATISTICS = EnumUtil.bitSetOf(Flag.SKIP_STATISTICS);
   public static final long OPERATION_HOTROD = EnumUtil.bitSetOf(Flag.OPERATION_HOTROD);
   public static final long OPERATION_MEMCACHED = EnumUtil.bitSetOf(Flag.OPERATION_MEMCACHED);
   public static final long SKIP_INDEX_CLEANUP = EnumUtil.bitSetOf(Flag.SKIP_INDEX_CLEANUP);
   public static final long COMMAND_RETRY = EnumUtil.bitSetOf(Flag.COMMAND_RETRY);
   public static final long ROLLING_UPGRADE = EnumUtil.bitSetOf(Flag.ROLLING_UPGRADE);
   public static final long REMOTE_ITERATION = EnumUtil.bitSetOf(Flag.REMOTE_ITERATION);
   public static final long WITH_INVOCATION_RECORDS = EnumUtil.bitSetOf(Flag.WITH_INVOCATION_RECORDS);

   /**
    * Creates a copy of a Flag BitSet removing instances of FAIL_SILENTLY.
    */
   public static long copyWithoutRemotableFlags(long flagsBitSet) {
      return EnumUtil.diffBitSets(flagsBitSet, FAIL_SILENTLY);
   }

   public static Flag extractStateTransferFlag(InvocationContext ctx, FlagAffectedCommand command) {
      if (command == null) {
         //commit command
         return ctx instanceof TxInvocationContext ?
               ((TxInvocationContext) ctx).getCacheTransaction().getStateTransferFlag() :
               null;
      } else {
         if (command.hasAnyFlag(FlagBitSets.PUT_FOR_STATE_TRANSFER)) {
            return Flag.PUT_FOR_STATE_TRANSFER;
         } else if (command.hasAnyFlag(FlagBitSets.PUT_FOR_X_SITE_STATE_TRANSFER)) {
            return Flag.PUT_FOR_X_SITE_STATE_TRANSFER;
         }
      }
      return null;
   }
}
