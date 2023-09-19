package org.infinispan.commands.irac;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.commons.util.Util;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.remoting.transport.Address;
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
public class IracTombstoneRemoteSiteCheckCommand extends BaseRpcCommand {

   public static final byte COMMAND_ID = 38;

   private List<Object> keys;

   @SuppressWarnings("unused")
   public IracTombstoneRemoteSiteCheckCommand() {
      super(null);
   }

   public IracTombstoneRemoteSiteCheckCommand(ByteString cacheName) {
      super(cacheName);
   }

   public IracTombstoneRemoteSiteCheckCommand(ByteString cacheName, List<Object> keys) {
      super(cacheName);
      this.keys = keys;
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
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public boolean isReturnValueExpected() {
      return true;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      MarshallUtil.marshallCollection(keys, output);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      keys = MarshallUtil.unmarshallCollection(input, ArrayList::new);
   }

   @Override
   public Address getOrigin() {
      //not needed
      return null;
   }

   @Override
   public void setOrigin(Address origin) {
      //no-op
   }

   @Override
   public String toString() {
      return "IracSiteTombstoneCheckCommand{" +
            "cacheName=" + cacheName +
            ", keys=" + keys.stream().map(Util::toStr).collect(Collectors.joining(",")) +
            '}';
   }

}
