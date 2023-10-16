package org.infinispan.xsite.commands.remote;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PrimitiveIterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import org.infinispan.commands.irac.IracTombstoneRemoteSiteCheckCommand;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.commons.util.Util;
import org.infinispan.distribution.DistributionInfo;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.remoting.responses.ValidResponse;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.ValidSingleResponseCollector;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.AggregateCompletionStage;
import org.infinispan.util.concurrent.CompletionStages;
import org.infinispan.xsite.irac.IracManager;

/**
 * A {@link XSiteCacheRequest} to check tombstones for IRAC algorithm.
 * <p>
 * Periodically, the primary owner sends this command to the remote sites where they check if the tombstone for this key
 * is still necessary.
 *
 * @since 15.0
 */
public class IracTombstoneCheckRequest extends XSiteCacheRequest<IntSet> {

   private List<Object> keys;

   public IracTombstoneCheckRequest() {
      super(null);
   }

   public IracTombstoneCheckRequest(ByteString cacheName, List<Object> keys) {
      super(cacheName);
      this.keys = keys;
   }

   @Override
   protected CompletionStage<IntSet> invokeInLocalCache(String origin, ComponentRegistry registry) {
      LocalizedCacheTopology topology = registry.getDistributionManager().getCacheTopology();
      IracManager iracManager = registry.getIracManager().running();
      RpcManager rpcManager = registry.getRpcManager().running();
      RpcOptions rpcOptions = rpcManager.getSyncRpcOptions();
      int numberOfKeys = keys.size();

      Map<Address, IntSetResponseCollector> primaryOwnerKeys = new HashMap<>(rpcManager.getMembers().size());
      IntSet toKeepIndexes = IntSets.concurrentSet(numberOfKeys);

      for (int index = 0; index < numberOfKeys; ++index) {
         Object key = keys.get(index);
         DistributionInfo dInfo = topology.getDistribution(key);
         if (dInfo.isPrimary()) {
            if (iracManager.containsKey(key)) {
               toKeepIndexes.set(index);
            }
         } else {
            IntSetResponseCollector collector = primaryOwnerKeys.computeIfAbsent(dInfo.primary(), a -> new IntSetResponseCollector(numberOfKeys, toKeepIndexes));
            collector.add(index, key);
         }
      }
      if (primaryOwnerKeys.isEmpty()) {
         return CompletableFuture.completedFuture(toKeepIndexes);
      }

      AggregateCompletionStage<IntSet> stage = CompletionStages.aggregateCompletionStage(toKeepIndexes);
      for (Map.Entry<Address, IntSetResponseCollector> entry : primaryOwnerKeys.entrySet()) {
         IracTombstoneRemoteSiteCheckCommand cmd = new IracTombstoneRemoteSiteCheckCommand(cacheName, entry.getValue().getKeys());
         stage.dependsOn(rpcManager.invokeCommand(entry.getKey(), cmd, entry.getValue(), rpcOptions));
      }
      return stage.freeze();

   }

   @Override
   public byte getCommandId() {
      return Ids.IRAC_TOMBSTONE_CHECK;
   }


   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      MarshallUtil.marshallCollection(keys, output);
      super.writeTo(output);
   }

   @Override
   public XSiteRequest<IntSet> readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      keys = MarshallUtil.unmarshallCollection(input, ArrayList::new);
      return super.readFrom(input);
   }


   @Override
   public String toString() {
      return "IracSiteTombstoneCheckCommand{" +
            "cacheName=" + cacheName +
            ", keys=" + keys.stream().map(Util::toStr).collect(Collectors.joining(",")) +
            '}';
   }

   private static class IntSetResponseCollector extends ValidSingleResponseCollector<Void> {

      private final List<Object> keys;
      private final int[] keyIndexes;
      private final IntSet globalToKeepIndexes;
      private int nextInsertPosition;

      private IntSetResponseCollector(int maxCapacity, IntSet globalToKeepIndexes) {
         keys = new ArrayList<>(maxCapacity);
         keyIndexes = new int[maxCapacity];
         this.globalToKeepIndexes = globalToKeepIndexes;
      }

      void add(int index, Object key) {
         assert nextInsertPosition < keyIndexes.length;
         keys.add(key);
         keyIndexes[nextInsertPosition++] = index;
      }

      List<Object> getKeys() {
         return keys;
      }

      @Override
      protected Void withValidResponse(Address sender, ValidResponse response) {
         Object rsp = response.getResponseValue();
         assert rsp instanceof IntSet;
         IntSet toKeep = (IntSet) rsp;

         for (PrimitiveIterator.OfInt it = toKeep.iterator(); it.hasNext(); ) {
            int localPosition = it.nextInt();
            assert localPosition < keyIndexes.length;
            globalToKeepIndexes.set(keyIndexes[localPosition]);
         }
         return null;
      }

      @Override
      protected Void withException(Address sender, Exception exception) {
         markAllToKeep();
         return null;
      }

      @Override
      protected Void targetNotFound(Address sender) {
         markAllToKeep();
         return null;
      }

      private void markAllToKeep() {
         for (int index : keyIndexes) {
            globalToKeepIndexes.set(index);
         }
      }
   }
}
