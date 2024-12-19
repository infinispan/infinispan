package org.infinispan.commands.remote;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.read.GetAllCommand;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.marshall.protostream.impl.MarshallableCollection;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.remoting.responses.Response;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.ByteString;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Issues a remote getAll call.  This is not a {@link org.infinispan.commands.VisitableCommand} and hence not passed up
 * the interceptor chain.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@ProtoTypeId(ProtoStreamTypeIds.CLUSTERED_GET_ALL_COMMAND)
public class ClusteredGetAllCommand<K, V> extends BaseClusteredReadCommand {
   public static final byte COMMAND_ID = 46;
   private static final Log log = LogFactory.getLog(ClusteredGetAllCommand.class);

   private List<?> keys;
   private GlobalTransaction gtx;

   public ClusteredGetAllCommand(ByteString cacheName, List<?> keys, long flags, GlobalTransaction gtx) {
      super(cacheName, -1, flags);
      this.keys = keys;
      this.gtx = gtx;
   }

   @ProtoFactory
   ClusteredGetAllCommand(ByteString cacheName, int topologyId, long flagsWithoutRemote, MarshallableCollection<?> wrappedKeys,
                          GlobalTransaction globalTransaction) {
      super(cacheName, topologyId, flagsWithoutRemote);
      this.keys = MarshallableCollection.unwrap(wrappedKeys, ArrayList::new);
      this.gtx = globalTransaction;
   }

   @ProtoField(number = 4, name = "keys")
   MarshallableCollection<?> getWrappedKeys() {
      return MarshallableCollection.create(keys);
   }

   @ProtoField(number = 5)
   GlobalTransaction getGlobalTransaction() {
      return gtx;
   }

   @Override
   public CompletionStage<?> invokeAsync(ComponentRegistry componentRegistry) throws Throwable {
      if (!EnumUtil.containsAny(flags, FlagBitSets.FORCE_WRITE_LOCK)) {
         return invokeGetAll(componentRegistry);
      } else {
         return componentRegistry.getCommandsFactory()
               .buildLockControlCommand(keys, flags, gtx)
               .invokeAsync(componentRegistry)
               .thenCompose(o -> invokeGetAll(componentRegistry));
      }
   }

   private CompletionStage<Object> invokeGetAll(ComponentRegistry cr) {
      // make sure the get command doesn't perform a remote call
      // as our caller is already calling the ClusteredGetCommand on all the relevant nodes
      GetAllCommand command = cr.getCommandsFactory().buildGetAllCommand(keys, flags, true);
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
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      ClusteredGetAllCommand<?, ?> that = (ClusteredGetAllCommand<?, ?>) o;
      return Objects.equals(keys, that.keys) &&
            Objects.equals(gtx, that.gtx);
   }

   @Override
   public int hashCode() {
      return Objects.hash(keys, gtx);
   }

   @Override
   public String toString() {
      return "ClusteredGetAllCommand{" + "keys=" + keys +
            ", flags=" + EnumUtil.prettyPrintBitSet(flags, Flag.class) +
            ", topologyId=" + topologyId +
            '}';
   }
}
