package org.infinispan.commands.remote;

import java.io.IOException;
import java.io.ObjectInput;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.read.GetAllCommand;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.container.impl.InternalEntryFactory;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextFactory;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.marshall.MarshalledEntryUtil;
import org.infinispan.marshall.core.MarshalledEntryFactory;
import org.infinispan.marshall.core.UserAwareObjectOutput;
import org.infinispan.remoting.responses.Response;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.ByteString;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Issues a remote getAll call.  This is not a {@link org.infinispan.commands.VisitableCommand} and hence not passed up the
 * interceptor chain.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class ClusteredGetAllCommand<K, V> extends BaseClusteredReadCommand {
   public static final byte COMMAND_ID = 46;
   private static final Log log = LogFactory.getLog(ClusteredGetAllCommand.class);
   private static final boolean trace = log.isTraceEnabled();

   private List<?> keys;
   private GlobalTransaction gtx;

   private InvocationContextFactory icf;
   private CommandsFactory commandsFactory;
   private AsyncInterceptorChain invoker;
   private TransactionTable txTable;
   private InternalEntryFactory entryFactory;

   ClusteredGetAllCommand() {
      super(null, EnumUtil.EMPTY_BIT_SET);
   }

   public ClusteredGetAllCommand(ByteString cacheName) {
      super(cacheName, EnumUtil.EMPTY_BIT_SET);
   }

   public ClusteredGetAllCommand(ByteString cacheName, List<?> keys, long flags, GlobalTransaction gtx) {
      super(cacheName, flags);
      this.keys = keys;
      this.gtx = gtx;
   }

   public void init(InvocationContextFactory icf, CommandsFactory commandsFactory, InternalEntryFactory entryFactory,
                    AsyncInterceptorChain interceptorChain, TransactionTable txTable) {
      this.icf = icf;
      this.commandsFactory = commandsFactory;
      this.invoker = interceptorChain;
      this.txTable = txTable;
      this.entryFactory = entryFactory;
   }

   @Override
   public CompletableFuture<Object> invokeAsync() throws Throwable {
      if (!hasAnyFlag(FlagBitSets.FORCE_WRITE_LOCK)) {
         return invokeGetAll();
      } else {
         return acquireLocks().thenCompose(o -> invokeGetAll());
      }
   }

   private CompletableFuture<Object> invokeGetAll() {
      // make sure the get command doesn't perform a remote call
      // as our caller is already calling the ClusteredGetCommand on all the relevant nodes
      GetAllCommand command = commandsFactory.buildGetAllCommand(keys, getFlagsBitSet(), true);
      command.setTopologyId(topologyId);
      InvocationContext invocationContext = icf.createRemoteInvocationContextForCommand(command, getOrigin());
      CompletableFuture<Object> future = invoker.invokeAsync(invocationContext, command);
      return future.thenApply(rv -> {
         if (trace) log.trace("Found: " + rv);
         if (rv == null || rv instanceof Response) {
            return rv;
         }

         Map<K, CacheEntry<K, V>> map = (Map<K, CacheEntry<K, V>>) rv;
         InternalCacheValue<V>[] values = new InternalCacheValue[keys.size()];
         int i = 0;
         for (Object key : keys) {
            CacheEntry<K, V> entry = map.get(key);
            InternalCacheValue<V> value;
            if (entry == null) {
               value = null;
            } else if (entry instanceof InternalCacheEntry) {
               value = ((InternalCacheEntry<K, V>) entry).toInternalCacheValue();
            } else {
               value = entryFactory.createValue(entry);
            }
            values[i++] = value;
         }
         return values;
      });
   }

   private CompletableFuture<Object> acquireLocks() throws Throwable {
      LockControlCommand lockControlCommand = commandsFactory.buildLockControlCommand(keys, getFlagsBitSet(), gtx);
      lockControlCommand.init(invoker, icf, txTable);
      return lockControlCommand.invokeAsync();
   }

   public List<?> getKeys() {
      return keys;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void writeTo(UserAwareObjectOutput output, MarshalledEntryFactory entryFactory) throws IOException {
      output.marshallCollection(keys, UserAwareObjectOutput::writeKey);
      output.writeLong(FlagBitSets.copyWithoutRemotableFlags(getFlagsBitSet()));
      output.writeObject(gtx);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      keys = MarshallUtil.unmarshallCollection(input, ArrayList::new, MarshalledEntryUtil::readKey);
      setFlagsBitSet(input.readLong());
      gtx = (GlobalTransaction) input.readObject();
   }

   @Override
   public boolean isReturnValueExpected() {
      return true;
   }

   @Override
   public String toString() {
      final StringBuilder sb = new StringBuilder("ClusteredGetAllCommand{");
      sb.append("keys=").append(keys);
      sb.append(", flags=").append(printFlags());
      sb.append(", topologyId=").append(topologyId);
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
      result = prime * result + ((keys == null) ? 0 : keys.hashCode());
      return result;
   }
}
