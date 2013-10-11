package org.infinispan.commands.write;

import org.infinispan.metadata.Metadata;
import org.infinispan.commands.AbstractFlagAffectedCommand;
import org.infinispan.commands.MetadataAwareCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.notifications.cachelistener.CacheNotifier;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class PutMapCommand extends AbstractFlagAffectedCommand implements WriteCommand, MetadataAwareCommand {
   public static final byte COMMAND_ID = 9;

   Map<Object, Object> map;
   CacheNotifier notifier;
   Metadata metadata;
   boolean isForwarded = false;

   public PutMapCommand() {
   }

   public PutMapCommand(Map<?, ?> map, CacheNotifier notifier, Metadata metadata, Set<Flag> flags) {
      this.map = (Map<Object, Object>) map;
      this.notifier = notifier;
      this.metadata = metadata;
      this.flags = flags;
   }

   public void init(CacheNotifier notifier) {
      this.notifier = notifier;
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitPutMapCommand(ctx, this);
   }

   private MVCCEntry lookupMvccEntry(InvocationContext ctx, Object key) {
      return (MVCCEntry) ctx.lookupEntry(key);
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      Map<Object, Object> previousValues = new HashMap<Object, Object>();
      for (Entry<Object, Object> e : map.entrySet()) {
         Object key = e.getKey();
         MVCCEntry me = lookupMvccEntry(ctx, key);
         if (me != null) {
            Object value = me.getValue();
            previousValues.put(key, value);
            notifier.notifyCacheEntryModified(
                  key, value, value == null, true, ctx, this);
            me.setValue(e.getValue());
            me.setChanged(true);
         }
      }
      return previousValues;
   }

   public Map<Object, Object> getMap() {
      return map;
   }

   public void setMap(Map<Object, Object> map) {
      this.map = map;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public Object[] getParameters() {
      return new Object[]{map, metadata, isForwarded, Flag.copyWithoutRemotableFlags(flags)};
   }

   @Override
   public void setParameters(int commandId, Object[] parameters) {
      map = (Map<Object, Object>) parameters[0];
      metadata = (Metadata) parameters[1];
      isForwarded = (Boolean) parameters[2];
      if (parameters.length > 3) {
         this.flags = (Set<Flag>) parameters[3];
      }
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      PutMapCommand that = (PutMapCommand) o;

      if (metadata != null ? !metadata.equals(that.metadata) : that.metadata != null) return false;
      if (map != null ? !map.equals(that.map) : that.map != null) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = map != null ? map.hashCode() : 0;
      result = 31 * result + (metadata != null ? metadata.hashCode() : 0);
      return result;
   }

   @Override
   public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("PutMapCommand{map={");
      if (!map.isEmpty()) {
         Iterator<Entry<Object, Object>> it = map.entrySet().iterator();
         int i = 0;
         for (;;) {
            Entry<Object, Object> e = it.next();
            sb.append(e.getKey()).append('=').append(e.getValue());
            if (!it.hasNext()) {
               break;
            }
            if (i > 100) {
               sb.append(" ...");
               break;
            }
            sb.append(", ");
            i++;
         }
      }
      sb.append("}, flags=").append(flags)
         .append(", metadata=").append(metadata)
         .append(", isForwarded=").append(isForwarded)
         .append("}");
      return sb.toString();
   }

   @Override
   public boolean shouldInvoke(InvocationContext ctx) {
      return true;
   }

   @Override
   public boolean isSuccessful() {
      return true;
   }

   @Override
   public boolean isConditional() {
      return false;
   }

   @Override
   public boolean isIgnorePreviousValue() {
      return false;
   }

   @Override
   public void setIgnorePreviousValue(boolean ignorePreviousValue) {
   }

   @Override
   public Set<Object> getAffectedKeys() {
      return map.keySet();
   }

   @Override
   public boolean isReturnValueExpected() {
      return false;
   }

   @Override
   public boolean canBlock() {
      return true;
   }

   @Override
   public boolean ignoreCommandOnStatus(ComponentStatus status) {
      return false;
   }

   @Override
   public Metadata getMetadata() {
      return metadata;
   }

   @Override
   public void setMetadata(Metadata metadata) {
      this.metadata = metadata;
   }

   /**
    * For non transactional caches that support concurrent writes (default), the commands are forwarded between nodes,
    * e.g.:
    * - commands is executed on node A, but some of the keys should be locked on node B
    * - the command is send to the main owner (B)
    * - B tries to acquire lock on the keys it owns, then forwards the commands to the other owners as well
    * - at this last stage, the command has the "isForwarded" flag set to true.
    * @return
    */
   public boolean isForwarded() {
      return isForwarded;
   }

   /**
    * @see #isForwarded()
    */
   public void setForwarded(boolean forwarded) {
      isForwarded = forwarded;
   }
}
