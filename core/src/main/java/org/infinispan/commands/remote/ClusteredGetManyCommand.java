package org.infinispan.commands.remote;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.read.GetManyCommand;
import org.infinispan.commons.equivalence.Equivalence;
import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.container.InternalEntryFactory;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextFactory;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Issues a remote getMany call.  This is not a {@link org.infinispan.commands.VisitableCommand} and hence not passed up the
 * {@link org.infinispan.interceptors.base.CommandInterceptor} chain.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class ClusteredGetManyCommand extends LocalFlagAffectedRpcCommand {
   public static final byte COMMAND_ID = 45;
   private static final Log log = LogFactory.getLog(ClusteredGetManyCommand.class);
   private static final boolean trace = log.isTraceEnabled();

   private Object[] keys; // using array here for efficiency, and for having the keys ordered to optimize return values
   private GlobalTransaction gtx;

   private InvocationContextFactory icf;
   private CommandsFactory commandsFactory;
   private InterceptorChain invoker;
   private TransactionTable txTable;
   private InternalEntryFactory entryFactory;
   private Equivalence keyEquivalence;

   public ClusteredGetManyCommand(String cacheName) {
      super(cacheName, null);
   }

   public ClusteredGetManyCommand(String cacheName, Object[] keys, Set<Flag> flags, GlobalTransaction gtx, Equivalence keyEquivalence) {
      super(cacheName, flags);
      this.keys = keys;
      this.gtx = gtx;
      this.keyEquivalence = keyEquivalence;
   }

   public void init(InvocationContextFactory icf, CommandsFactory commandsFactory, InternalEntryFactory entryFactory,
                    InterceptorChain interceptorChain, TransactionTable txTable, Equivalence keyEquivalence) {
      this.icf = icf;
      this.commandsFactory = commandsFactory;
      this.invoker = interceptorChain;
      this.txTable = txTable;
      this.entryFactory = entryFactory;
      this.keyEquivalence = keyEquivalence;
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      acquireLocksIfNeeded();
      // make sure the get command doesn't perform a remote call
      // as our caller is already calling the ClusteredGetCommand on all the relevant nodes
      Set<Flag> commandFlags = EnumSet.of(Flag.SKIP_REMOTE_LOOKUP, Flag.CACHE_MODE_LOCAL);
      if (this.flags != null) commandFlags.addAll(this.flags);
      GetManyCommand command = commandsFactory.buildGetManyCommand(createSet(keys), commandFlags, true);
      InvocationContext invocationContext = icf.createRemoteInvocationContextForCommand(command, getOrigin());
      Map<Object, ? extends CacheEntry> map = (Map<Object, ? extends CacheEntry>) invoker.invoke(invocationContext, command);
      if (trace) log.trace("Found: " + map);

      //this might happen if the value was fetched from a cache loader
      InternalCacheValue[] values = new InternalCacheValue[keys.length];
      for (int i = 0; i < keys.length; ++i) {
         CacheEntry entry = map.get(keys[i]);
         if (entry == null) {
            values[i] = null;
         } else if (entry instanceof InternalCacheEntry) {
            values[i] = ((InternalCacheEntry) entry).toInternalCacheValue();
         } else {
            values[i] = entryFactory.createValue(entry);
         }
      }
      return values;
   }

   private void acquireLocksIfNeeded() throws Throwable {
      if (hasFlag(Flag.FORCE_WRITE_LOCK)) {
         LockControlCommand lockControlCommand = commandsFactory.buildLockControlCommand(createSet(keys), flags, gtx);
         lockControlCommand.init(invoker, icf, txTable);
         lockControlCommand.perform(null);
      }
   }

   public Object[] getKeys() {
      return keys;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public Object[] getParameters() {
      return new Object[] { keys, flags, gtx };
   }

   @Override
   public void setParameters(int commandId, Object[] parameters) {
      this.keys = (Object[]) parameters[0];
      this.flags = (Set<Flag>) parameters[1];
      this.gtx = (GlobalTransaction) parameters[2];
   }

   @Override
   public boolean isReturnValueExpected() {
      return true;
   }

   private Set<Object> createSet(Object[] elements) {
      HashSet<Object> set = new HashSet<>(elements.length);
      for (Object element : elements) set.add(element);
      return set;
   }

   @Override
   public String toString() {
      final StringBuilder sb = new StringBuilder("ClusteredGetManyCommand{");
      sb.append("keys=").append(Arrays.toString(keys));
      sb.append(", flags=").append(flags);
      sb.append('}');
      return sb.toString();
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      ClusteredGetManyCommand that = (ClusteredGetManyCommand) o;

      if (gtx != null ? !gtx.equals(that.gtx) : that.gtx != null) return false;
      if (keyEquivalence != null ? !keyEquivalence.equals(that.keyEquivalence) : that.keyEquivalence != null)
         return false;

      if (keys == null) {
         return that.keys == null;
      } else if (that.keys == null) {
         return false;
      } else if (keys.length != that.keys.length) {
         return false;
      } else {
         Set<Object> myKeys = CollectionFactory.makeSet(keyEquivalence);
         for (Object key : that.keys) {
            if (!myKeys.contains(key)) {
               return false;
            }
         }
      }

      return true;
   }

   @Override
   public int hashCode() {
      int result = 0;
      // we need hashCode regardless of key order
      for (Object key : keys) {
         result = result ^ key.hashCode();
      }
      result = 31 * result + (gtx != null ? gtx.hashCode() : 0);
      result = 31 * result + (keyEquivalence != null ? keyEquivalence.hashCode() : 0);
      return result;
   }
}
