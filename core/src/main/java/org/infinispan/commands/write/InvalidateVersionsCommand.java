package org.infinispan.commands.write;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commons.marshall.UserObjectInput;
import org.infinispan.commons.marshall.UserObjectOutput;
import org.infinispan.container.DataContainer;
import org.infinispan.container.versioning.InequalVersionComparisonResult;
import org.infinispan.container.versioning.SimpleClusteredVersion;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.marshall.core.MarshalledEntryFactory;
import org.infinispan.persistence.manager.OrderedUpdatesManager;
import org.infinispan.scattered.BiasManager;
import org.infinispan.statetransfer.StateTransferLock;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Must be {@link VisitableCommand} as we want to catch it in persistence handling etc.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class InvalidateVersionsCommand extends BaseRpcCommand {
   private static final Log log = LogFactory.getLog(InvalidateVersionsCommand.class);
   private static final boolean trace = log.isTraceEnabled();

   public static final int COMMAND_ID = 67;

   // This is the topologyId in which this command is valid (in case that it comes from the state transfer)
   private int topologyId;
   private Object[] keys;
   private int[] topologyIds;
   private long[] versions;
   // Removed means that the comparison will remove current versions as well
   private boolean removed;

   protected DataContainer dataContainer;
   protected OrderedUpdatesManager orderedUpdatesManager;
   protected StateTransferLock stateTransferLock;
   protected DistributionManager distributionManager;
   protected BiasManager biasManager;

   public InvalidateVersionsCommand() {
      this(null);
   }

   public InvalidateVersionsCommand(ByteString cacheName) {
      super(cacheName);
   }

   public InvalidateVersionsCommand(ByteString cacheName, int topologyId, Object[] keys, int[] topologyIds, long[] versions, boolean removed) {
      super(cacheName);
      this.topologyId = topologyId;
      this.keys = keys;
      this.topologyIds = topologyIds;
      this.versions = versions;
      this.removed = removed;
   }

   public void init(DataContainer dataContainer, OrderedUpdatesManager orderedUpdatesManager,
                    StateTransferLock stateTransferLock, DistributionManager distributionManager,
                    BiasManager biasManager) {
      this.dataContainer = dataContainer;
      this.orderedUpdatesManager = orderedUpdatesManager;
      this.stateTransferLock = stateTransferLock;
      this.distributionManager = distributionManager;
      this.biasManager = biasManager;
   }

   @Override
   public CompletableFuture<Object> invokeAsync() {
      if (stateTransferLock != null) {
         stateTransferLock.acquireSharedTopologyLock();
      }
      try {
         if (topologyId >= 0 && distributionManager != null) {
            int currentTopologyId = distributionManager.getCacheTopology().getTopologyId();
            if (topologyId > currentTopologyId) {
               if (trace) {
                  log.tracef("Delaying command %s, current topology id %d", this, currentTopologyId);
               }
               return stateTransferLock.topologyFuture(topologyId).thenCompose(nil -> invokeAsync());
            } else if (topologyId < currentTopologyId) {
               log.ignoringInvalidateVersionsFromOldTopology(this.topologyId, currentTopologyId);
               if (trace) {
                  log.tracef("Ignored command is %s", this);
               }
               return CompletableFutures.completedNull();
            }
         }
         for (int i = 0; i < keys.length; ++i) {
            Object key = keys[i];
            if (key == null) break;
            SimpleClusteredVersion version = new SimpleClusteredVersion(topologyIds[i], versions[i]);
            if (biasManager != null) {
               biasManager.revokeLocalBias(key);
            }
            dataContainer.compute(key, (k, oldEntry, factory) -> {
               if (oldEntry == null) {
                  return null;
               }
               SimpleClusteredVersion localVersion = (SimpleClusteredVersion) oldEntry.getMetadata().version();
               InequalVersionComparisonResult result = localVersion.compareTo(version);
               if (result == InequalVersionComparisonResult.BEFORE || (removed && result == InequalVersionComparisonResult.EQUAL)) {
                  return null;
               } else {
                  return oldEntry;
               }
            });
         }
      } finally {
         if (stateTransferLock != null) {
            stateTransferLock.releaseSharedTopologyLock();
         }
      }
      return orderedUpdatesManager.invalidate(keys).thenApply(nil -> null);
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public boolean isReturnValueExpected() {
      return false;
   }

   @Override
   public boolean canBlock() {
      return false;
   }

   @Override
   public void writeTo(UserObjectOutput output, MarshalledEntryFactory entryFactory) throws IOException {
      output.writeInt(topologyId);
      // TODO: topology ids are mostly the same - sort the arrays according to topologyIds and use compaction encoding
      output.writeInt(keys.length);
      for (int i = 0; i < keys.length; ++i) {
         output.writeKey(keys[i]);
         if (keys[i] == null)
            break;

         output.writeInt(topologyIds[i]);
         output.writeLong(versions[i]);
      }
      output.writeBoolean(removed);
   }

   @Override
   public void readFrom(UserObjectInput input) throws IOException, ClassNotFoundException {
      topologyId = input.readInt();
      keys = new Object[input.readInt()];
      topologyIds = new int[keys.length];
      versions = new long[keys.length];
      for (int i = 0; i < keys.length; ++i) {
         Object key = input.readUserObject();
         if (key == null) {
            break;
         }
         keys[i] = key;
         topologyIds[i] = input.readInt();
         versions[i] = input.readLong();
      }
      removed = input.readBoolean();
   }

   @Override
   public String toString() {
      StringBuilder sb = new StringBuilder("InvalidateVersionsCommand{topologyId=").append(topologyId)
            .append(", removed=").append(removed).append(": ");
      if (keys.length > 0 && keys[0] != null) {
         sb.append(keys[0]).append(" -> ").append(versions[0]);
      } else {
         sb.append("<no-keys>");
      }
      for (int i = 1; i < keys.length; ++i) {
         if (keys[i] == null) break;
         sb.append(", ").append(keys[i]).append(" -> ").append(versions[i]);
      }
      return sb.append("}").toString();
   }
}
