package org.infinispan.commands.remote;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.read.GetAllCommand;
import org.infinispan.commons.equivalence.Equivalence;
import org.infinispan.container.InternalEntryFactory;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.ImmortalCacheValue;
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
 * Issues a remote getAll call.  This is not a {@link org.infinispan.commands.VisitableCommand} and hence not passed up the
 * {@link org.infinispan.interceptors.base.CommandInterceptor} chain.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class ClusteredGetAllCommand<K, V> extends LocalFlagAffectedRpcCommand {
   public static final byte COMMAND_ID = 46;
   private static final Log log = LogFactory.getLog(ClusteredGetAllCommand.class);
   private static final boolean trace = log.isTraceEnabled();

   private List<?> keys;
   private GlobalTransaction gtx;

   private InvocationContextFactory icf;
   private CommandsFactory commandsFactory;
   private InterceptorChain invoker;
   private TransactionTable txTable;
   private InternalEntryFactory entryFactory;
   private Equivalence<? super K> keyEquivalence;

   ClusteredGetAllCommand() {
      super(null, null);
   }

   public ClusteredGetAllCommand(String cacheName) {
      super(cacheName, null);
   }

   public ClusteredGetAllCommand(String cacheName, List<?> keys, Set<Flag> flags,
         GlobalTransaction gtx, Equivalence<? super K> keyEquivalence) {
      super(cacheName, flags);
      this.keys = keys;
      this.gtx = gtx;
      this.keyEquivalence = keyEquivalence;
   }

   public void init(InvocationContextFactory icf, CommandsFactory commandsFactory,
         InternalEntryFactory entryFactory, InterceptorChain interceptorChain,
         TransactionTable txTable, Equivalence<? super K> keyEquivalence) {
      this.icf = icf;
      this.commandsFactory = commandsFactory;
      this.invoker = interceptorChain;
      this.txTable = txTable;
      this.entryFactory = entryFactory;
      this.keyEquivalence = keyEquivalence;
   }

   @SuppressWarnings("unchecked")
   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      acquireLocksIfNeeded();
      // make sure the get command doesn't perform a remote call
      // as our caller is already calling the ClusteredGetCommand on all the relevant nodes
      GetAllCommand command = commandsFactory.buildGetAllCommand(keys, flags, true);
      InvocationContext invocationContext = icf.createRemoteInvocationContextForCommand(command, getOrigin());
      Map<K, CacheEntry<K, V>> map = (Map<K, CacheEntry<K, V>>) invoker.invoke(invocationContext, command);
      if (trace) log.trace("Found: " + map);

      if (map == null) {
         return null;
      }

      List<InternalCacheValue<V>> values = new ArrayList<>(keys.size());
      for (Object key : keys) {
         if (map.containsKey(key)) {
            CacheEntry<K, V> entry = map.get(key);
            InternalCacheValue<V> value;
            if (entry instanceof InternalCacheEntry) {
               value = ((InternalCacheEntry<K, V>) entry).toInternalCacheValue();
            } else if (entry != null) {
               value = entryFactory.createValue(entry);
            } else {
               value = new ImmortalCacheValue(null);
            }
            values.add(value);
         } else {
            values.add(null);
         }
      }
      return values;
   }

   private void acquireLocksIfNeeded() throws Throwable {
      if (hasFlag(Flag.FORCE_WRITE_LOCK)) {
         LockControlCommand lockControlCommand = commandsFactory.buildLockControlCommand(keys, flags, gtx);
         lockControlCommand.init(invoker, icf, txTable);
         lockControlCommand.perform(null);
      }
   }

   public List<?> getKeys() {
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

   @SuppressWarnings("unchecked")
   @Override
   public void setParameters(int commandId, Object[] parameters) {
      this.keys = (List<Object>) parameters[0];
      this.flags = (Set<Flag>) parameters[1];
      this.gtx = (GlobalTransaction) parameters[2];
   }

   @Override
   public boolean isReturnValueExpected() {
      return true;
   }

   @Override
   public String toString() {
      final StringBuilder sb = new StringBuilder("ClusteredGetAllCommand{");
      sb.append("keys=").append(keys);
      sb.append(", flags=").append(flags);
      sb.append('}');
      return sb.toString();
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (getClass() != obj.getClass())
         return false;
      ClusteredGetAllCommand<?, ?> other = (ClusteredGetAllCommand<?, ?>) obj;
      if (gtx == null) {
         if (other.gtx != null)
            return false;
      } else if (!gtx.equals(other.gtx))
         return false;
      if (keyEquivalence == null) {
         if (other.keyEquivalence != null)
            return false;
      } else if (!keyEquivalence.equals(other.keyEquivalence))
         return false;
      if (keys == null) {
         if (other.keys != null)
            return false;
      } else if (!keys.equals(other.keys))
         return false;
      return true;
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((gtx == null) ? 0 : gtx.hashCode());
      result = prime * result + ((keyEquivalence == null) ? 0 : keyEquivalence.hashCode());
      result = prime * result + ((keys == null) ? 0 : keys.hashCode());
      return result;
   }
}
