package org.infinispan.commands.write;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.container.DataContainer;
import org.infinispan.container.versioning.InequalVersionComparisonResult;
import org.infinispan.container.versioning.SimpleClusteredVersion;
import org.infinispan.persistence.manager.OrderedUpdatesManager;
import org.infinispan.util.ByteString;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.CompletableFuture;

/**
 * Must be {@link VisitableCommand} as we want to catch it in persistence handling etc.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class InvalidateVersionsCommand extends BaseRpcCommand {
   public static final int COMMAND_ID = 67;

   private Object[] keys;
   private int[] topologyIds;
   private long[] versions;
   // Removed means that the comparison will remove current versions as well
   private boolean removed;

   protected DataContainer dataContainer;
   protected OrderedUpdatesManager orderedUpdatesManager;

   public InvalidateVersionsCommand() {
      this(null);
   }

   public InvalidateVersionsCommand(ByteString cacheName) {
      super(cacheName);
   }

   public InvalidateVersionsCommand(ByteString cacheName, Object[] keys, int[] topologyIds, long[] versions, boolean removed) {
      super(cacheName);
      this.keys = keys;
      this.topologyIds = topologyIds;
      this.versions = versions;
      this.removed = removed;
   }

   public void init(DataContainer dataContainer, OrderedUpdatesManager orderedUpdatesManager) {
      this.dataContainer = dataContainer;
      this.orderedUpdatesManager = orderedUpdatesManager;
   }

   @Override
   public CompletableFuture<Object> invokeAsync() {
      for (int i = 0; i < keys.length; ++i) {
         Object key = keys[i];
         if (key == null) break;
         SimpleClusteredVersion version = new SimpleClusteredVersion(topologyIds[i], versions[i]);
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
   public void writeTo(ObjectOutput output) throws IOException {
      // TODO: topology ids are mostly the same - sort the arrays according to topologyIds and use compaction encoding
      output.writeInt(keys.length);
      for (int i = 0; i < keys.length; ++i) {
         if (keys[i] == null) {
            output.writeObject(null);
            break;
         } else {
            output.writeObject(keys[i]);
            output.writeInt(topologyIds[i]);
            output.writeLong(versions[i]);
         }
      }
      output.writeBoolean(removed);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      keys = new Object[input.readInt()];
      topologyIds = new int[keys.length];
      versions = new long[keys.length];
      for (int i = 0; i < keys.length; ++i) {
         Object key = input.readObject();
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
      StringBuilder sb = new StringBuilder("InvalidateVersionsCommand{removed=").append(removed).append(": ");
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
