package org.infinispan.commands.read;

import org.infinispan.commands.AbstractFlagAffectedCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.container.InternalEntryFactory;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import static org.infinispan.commons.util.Util.toStr;

/**
 * Retrieves multiple entries at once.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
// TODO: revise the command hierarchy, e.g. this should not implement MetadataAwareCommand
public class GetAllCommand extends AbstractFlagAffectedCommand {
   public static final byte COMMAND_ID = 44;
   private static final Log log = LogFactory.getLog(GetAllCommand.class);
   private static final boolean trace = log.isTraceEnabled();

   private Collection<?> keys;
   private boolean returnEntries;
   private ConsistentHash ch;
   private Address localAddress;

   // TODO: remotely fetched are because of compatibility - can't we just always return InternalCacheEntry and have
   //       the unboxing executed as the topmost interceptor?
   private Map<Object, InternalCacheEntry> remotelyFetched;

   private /* transient */ InternalEntryFactory entryFactory;

   public GetAllCommand(Collection<?> keys, Set<Flag> flags,
         boolean returnEntries, InternalEntryFactory entryFactory) {
      this.keys = keys;
      this.flags = flags;
      this.returnEntries = returnEntries;
      this.entryFactory = entryFactory;
   }

   GetAllCommand() {
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitGetAllCommand(ctx, this);
   }

   @Override
   public boolean shouldInvoke(InvocationContext ctx) {
      return true;
   }

   @Override
   public boolean ignoreCommandOnStatus(ComponentStatus status) {
      // TODO: this should only be ignored when it is ran in a remote context
      return status != ComponentStatus.RUNNING && status != ComponentStatus.INITIALIZING;
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      Map<Object, Object> map = createMap();
      for (Object key : keys) {
         CacheEntry entry = ctx.lookupEntry(key);
         if (entry == null) {
            if (trace) {
               log.tracef("Entry for key %s not found", key);
            }
            continue;
         }
         if (entry.isNull()) {
            if (trace) {
               log.tracef("Entry for key %s is null in current context", key);
            }
            map.put(key, null);
            continue;
         }
         if (entry.isRemoved()) {
            if (trace) {
               log.tracef("Entry for key %s has been deleted and is of type %s", key, entry.getClass().getSimpleName());
            }
            map.put(key, null);
            continue;
         }

         // Get cache entry instead of value
         if (returnEntries) {
            CacheEntry copy;
            if (ctx.isOriginLocal()) {
               copy = entryFactory.copy(entry);
            } else {
               copy = entry;
            }
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
      return new LinkedHashMap<>();
   }

   private Set<Object> createSet(Object[] elements) {
      HashSet<Object> set = new HashSet<>(elements.length);
      for (Object element : elements) set.add(element);
      return set;
   }

   public Collection<?> getKeys() {
      return keys;
   }

   public void setKeys(Collection<?> keys) {
      this.keys = keys;
   }

   public Map<Object, InternalCacheEntry> getRemotelyFetched() {
      return remotelyFetched;
   }

   public void setRemotelyFetched(Map<Object, InternalCacheEntry> remotelyFetched) {
      this.remotelyFetched = remotelyFetched;
   }

   public void setConsistentHashAndAddress(ConsistentHash ch, Address localAddress) {
      this.ch = ch;
      this.localAddress = localAddress;
   }

   public ConsistentHash getConsistentHash() {
      return ch;
   }

   @Override
   public String toString() {
      final StringBuilder sb = new StringBuilder("GetAllCommand{");
      sb.append("keys=").append(keys);
      sb.append(", returnEntries=").append(returnEntries);
      sb.append(", flags=").append(flags);
      sb.append('}');
      return sb.toString();
   }
}
