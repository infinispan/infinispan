package org.infinispan.commands.read;

import static org.infinispan.commons.util.Util.toStr;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.infinispan.commands.AbstractFlagAffectedCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.container.InternalEntryFactory;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Retrieves multiple entries at once.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
// TODO: revise the command hierarchy, e.g. this should not implement MetadataAwareCommand
public class GetManyCommand extends AbstractFlagAffectedCommand {
   public static final byte COMMAND_ID = 44;
   private static final Log log = LogFactory.getLog(GetManyCommand.class);
   private static final boolean trace = log.isTraceEnabled();

   private Set<Object> keys;
   private boolean returnEntries;
   // TODO: primary only/staggered gets policy

   // TODO: remotely fetched are because of compatibility - can't we just always return InternalCacheEntry and have
   //       the unboxing executed as the topmost interceptor?
   private Map<Object, InternalCacheEntry> remotelyFetched;

   private /* transient */ InternalEntryFactory entryFactory;

   public GetManyCommand(Set<?> keys, Set<Flag> flags, boolean returnEntries, InternalEntryFactory entryFactory) {
      this.keys = (Set<Object>) keys;
      this.flags = flags;
      this.returnEntries = returnEntries;
      this.entryFactory = entryFactory;
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitGetManyCommand(ctx, this);
   }

   @Override
   public boolean shouldInvoke(InvocationContext ctx) {
      return true;
   }

   @Override
   public boolean ignoreCommandOnStatus(ComponentStatus status) {
      return false;
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      Map<Object, Object> map = createMap();
      for (Object key : keys) {
         CacheEntry entry = ctx.lookupEntry(key);
         if (entry == null || entry.isNull()) {
            if (trace) {
               log.tracef("Entry for key %s not found", key);
            }
            continue;
         }
         if (entry.isRemoved()) {
            if (trace) {
               log.tracef("Entry for key %s has been deleted and is of type %s", key, entry.getClass().getSimpleName());
            }
            continue;
         }

         // Get cache entry instead of value
         if (returnEntries) {
            CacheEntry copy = entryFactory.copy(entry);
            if (trace) {
               log.tracef("Found entry %s -> %s", key, entry);
               log.tracef("Returning copied entry %s", copy);
            }
            map.put(key, copy);
         } else {
            Object value = entry.getValue();
            if (trace) {
               log.tracef("Found %s -> %s", key, toStr(value));
            }
            map.put(key, value);
         }
      }
      return map;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public Object[] getParameters() {
      return new Object[]{ keys.toArray(), Flag.copyWithoutRemotableFlags(flags), returnEntries};
   }

   @Override
   public void setParameters(int commandId, Object[] parameters) {
      if (commandId != COMMAND_ID) throw new IllegalStateException("Invalid method id");
      keys = createSet((Object[]) parameters[0]);
      flags = (Set<Flag>) parameters[1];
      returnEntries = (Boolean) parameters[2];
   }

   @Override
   public boolean isReturnValueExpected() {
      return true;
   }

   @Override
   public boolean canBlock() {
      return false;
   }

   public boolean isReturnEntries() {
      return returnEntries;
   }

   public <V> Map<Object, V> createMap() {
      // TODO: HashMap is not optimal for serialization
      return new HashMap<>();
   }

   private Set<Object> createSet(Object[] elements) {
      HashSet<Object> set = new HashSet<>(elements.length);
      for (Object element : elements) set.add(element);
      return set;
   }

   public Set<Object> getKeys() {
      return keys;
   }

   public void setKeys(Set<Object> keys) {
      this.keys = keys;
   }
   public Map<Object, InternalCacheEntry> getRemotelyFetched() {
      return remotelyFetched;
   }

   public void setRemotelyFetched(Map<Object, InternalCacheEntry> remotelyFetched) {
      this.remotelyFetched = remotelyFetched;
   }

   @Override
   public String toString() {
      final StringBuilder sb = new StringBuilder("GetManyCommand{");
      sb.append("keys=").append(keys);
      sb.append(", returnEntries=").append(returnEntries);
      sb.append(", flags=").append(flags);
      sb.append('}');
      return sb.toString();
   }
}
