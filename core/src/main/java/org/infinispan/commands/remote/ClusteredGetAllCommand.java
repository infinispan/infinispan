package org.infinispan.commands.remote;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.read.GetAllCommand;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.remoting.responses.Response;
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

   private List<?> keys;
   private GlobalTransaction gtx;


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

   @Override
   public CompletionStage<?> invokeAsync(ComponentRegistry componentRegistry) throws Throwable {
      if (!hasAnyFlag(FlagBitSets.FORCE_WRITE_LOCK)) {
         return invokeGetAll(componentRegistry);
      } else {
         return componentRegistry.getCommandsFactory()
               .buildLockControlCommand(keys, getFlagsBitSet(), gtx)
               .invokeAsync(componentRegistry)
               .thenCompose(o -> invokeGetAll(componentRegistry));
      }
   }

   private CompletionStage<Object> invokeGetAll(ComponentRegistry cr) {
      // make sure the get command doesn't perform a remote call
      // as our caller is already calling the ClusteredGetCommand on all the relevant nodes
      GetAllCommand command = cr.getCommandsFactory().buildGetAllCommand(keys, getFlagsBitSet(), true);
      command.setTopologyId(topologyId);
      InvocationContext invocationContext = cr.getInvocationContextFactory().running().createRemoteInvocationContextForCommand(command, getOrigin());
      CompletionStage<Object> future = cr.getInterceptorChain().running().invokeAsync(invocationContext, command);
      return future.thenApply(rv -> {
         if (log.isTraceEnabled()) log.trace("Found: " + rv);
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
               value = cr.getInternalEntryFactory().running().createValue(entry);
               value.setInternalMetadata(entry.getInternalMetadata());
            }
            values[i++] = value;
         }
         return values;
      });
   }

   public List<?> getKeys() {
      return keys;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      MarshallUtil.marshallCollection(keys, output);
      output.writeLong(FlagBitSets.copyWithoutRemotableFlags(getFlagsBitSet()));
      output.writeObject(gtx);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      keys = MarshallUtil.unmarshallCollection(input, ArrayList::new);
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
