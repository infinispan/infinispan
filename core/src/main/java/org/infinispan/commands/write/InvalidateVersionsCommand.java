package org.infinispan.commands.write;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.container.DataContainer;
import org.infinispan.container.versioning.NumericVersion;
import org.infinispan.context.InvocationContext;
import org.infinispan.lifecycle.ComponentStatus;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Must be {@link VisitableCommand} as we want to catch it in persistence handling etc.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class InvalidateVersionsCommand implements VisitableCommand {
   public static final int COMMAND_ID = 61;
   protected Object[] keys;
   protected long[] versions;
   // Removed means that the comparison will remove current versions as well
   boolean removed;

   protected DataContainer dataContainer;

   public InvalidateVersionsCommand() {
   }

   public InvalidateVersionsCommand(Object[] keys, long[] versions, boolean removed) {
      this.keys = keys;
      this.versions = versions;
      this.removed = removed;
   }

   public void init(DataContainer dataContainer) {
      this.dataContainer = dataContainer;
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitInvalidateVersionsCommand(ctx, this);
   }

   @Override
   public boolean shouldInvoke(InvocationContext ctx) {
      return true;
   }

   @Override
   public boolean ignoreCommandOnStatus(ComponentStatus status) {
      return status != ComponentStatus.RUNNING && status != ComponentStatus.INITIALIZING;
   }

   @Override
   public boolean readsExistingValues() {
      return true;
   }

   @Override
   public Object perform(InvocationContext ctx) {
      for (int i = 0; i < keys.length; ++i) {
         Object key = keys[i];
         if (key == null) break;
         long version = versions[i];
         dataContainer.compute(key, (k, oldEntry, factory) -> {
            if (oldEntry == null) {
               return oldEntry;
            }
            long localVersion = ((NumericVersion) oldEntry.getMetadata().version()).getVersion();
            if (localVersion < version || (removed && localVersion == version)) {
               return null;
            } else {
               return oldEntry;
            }
         });
      }
      return null;
   }

   public Object[] getKeys() {
      return keys;
   }

   public long[] getVersions() {
      return versions;
   }

   public boolean isRemoved() {
      return removed;
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
      output.writeInt(keys.length);
      for (int i = 0; i < keys.length; ++i) {
         if (keys[i] == null) {
            output.writeObject(null);
            break;
         } else {
            output.writeObject(keys[i]);
            output.writeLong(versions[i]);
         }
      }
      output.writeBoolean(removed);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      keys = new Object[input.readInt()];
      versions = new long[keys.length];
      for (int i = 0; i < keys.length; ++i) {
         Object key = input.readObject();
         if (key == null) {
            break;
         }
         keys[i] = key;
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
