package org.infinispan.commands.irac;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.commons.util.Util;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.marshall.protostream.impl.MarshallableList;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.util.ByteString;
import org.infinispan.xsite.irac.IracManager;

/**
 * A {@link BaseRpcCommand} to check tombstones for IRAC algorithm.
 * <p>
 * Periodically, the primary owner sends this command to the remote sites where they check if the tombstone for this key
 * is still necessary.
 *
 * @since 14.0
 */
@ProtoTypeId(ProtoStreamTypeIds.IRAC_TOMBSTONE_REMOTE_SITE_CHECK_COMMAND)
public class IracTombstoneRemoteSiteCheckCommand extends BaseIracCommand {

   final List<Object> keys;

   public IracTombstoneRemoteSiteCheckCommand(ByteString cacheName, List<Object> keys) {
      super(cacheName);
      this.keys = keys;
   }

   @ProtoFactory
   IracTombstoneRemoteSiteCheckCommand(ByteString cacheName, MarshallableList<Object> wrappedKeys) {
      super(cacheName);
      this.keys = MarshallableList.unwrap(wrappedKeys);
   }

   @ProtoField(2)
   MarshallableList<Object> getWrappedKeys()  {
      return MarshallableList.create(keys);
   }

   @Override
   public CompletionStage<IntSet> invokeAsync(ComponentRegistry registry) {
      int numberOfKeys = keys.size();
      IntSet toKeepIndexes = IntSets.mutableEmptySet(numberOfKeys);
      LocalizedCacheTopology topology = registry.getDistributionManager().getCacheTopology();
      IracManager iracManager = registry.getIracManager().running();
      for (int index = 0; index < numberOfKeys; ++index) {
         Object key = keys.get(index);
         // if we are not the primary owner mark the tombstone to keep
         // if we have a pending update to send, mark the tombstone to keep
         if (!topology.getDistribution(key).isPrimary() || iracManager.containsKey(key)) {
            toKeepIndexes.set(index);
         }
      }
      return CompletableFuture.completedFuture(toKeepIndexes);
   }

   @Override
   public boolean isReturnValueExpected() {
      return true;
   }

   @Override
   public String toString() {
      return "IracSiteTombstoneCheckCommand{" +
            "cacheName=" + cacheName +
            ", keys=" + keys.stream().map(Util::toStr).collect(Collectors.joining(",")) +
            '}';
   }
}
