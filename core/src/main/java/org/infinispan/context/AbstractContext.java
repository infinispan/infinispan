package org.infinispan.context;

import org.infinispan.container.entries.CacheEntry;
import org.infinispan.util.BidirectionalLinkedHashMap;
import org.infinispan.util.BidirectionalMap;

import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

/**
 * Common features of transaction and invocation contexts
 *
 * @author Manik Surtani
 * @since 4.0
 */
public abstract class AbstractContext {

   protected volatile EnumSet<Flag> flags;

   // these flags pertain to the context and are set internally.  Not to be confused with Flag, which is set by user
   // invocations on AdvancedCache.
   protected byte contextFlags;
   protected BidirectionalLinkedHashMap<Object, CacheEntry> lookedUpEntries = null;

   protected static enum ContextFlags {
      FORCE_SYNCHRONOUS(1), FORCE_ASYNCHRONOUS(1 << 1), ORIGIN_LOCAL(1 << 2), LOCAL_ROLLBACK_ONLY(1 << 3),
      CONTAINS_MODS(1 << 4), CONTAINS_LOCKS(1 << 5);
      final byte mask;

      ContextFlags(int mask) {
         this.mask = (byte) mask;
      }
   }

   protected final boolean isContextFlagSet(ContextFlags flag) {
      return (contextFlags & flag.mask) != 0;
   }

   protected final void setContextFlag(ContextFlags flag) {
      contextFlags |= flag.mask;
   }

   protected final void unsetContextFlag(ContextFlags flag) {
      contextFlags &= ~flag.mask;
   }

   protected final void setContextFlag(ContextFlags flag, boolean value) {
      if (value)
         setContextFlag(flag);
      else
         unsetContextFlag(flag);
   }

   public boolean hasFlag(Flag o) {
      return flags != null && flags.contains(o);
   }

   public Set<Flag> getFlags() {
      return flags;
   }

   public void setFlags(Flag... flags) {
      if (flags == null || flags.length == 0) return;
      if (this.flags == null)
         this.flags = EnumSet.copyOf(Arrays.asList(flags));
      else
         this.flags.addAll(Arrays.asList(flags));
   }

   public void setFlags(Collection<Flag> flags) {
      if (flags == null || flags.size() == 0) return;
      if (this.flags == null)
         this.flags = EnumSet.copyOf(flags);
      else
         this.flags.addAll(flags);
   }

   public void resetFlags() {
      flags = null;
   }

   public boolean isFlagsUninitialized() {
      return flags == null;
   }

   protected abstract int getLockSetSize();

   public boolean hasLockedKey(Object key) {
      CacheEntry e = lookupEntry(key);
      return e != null && e.isChanged();
   }

   public CacheEntry lookupEntry(Object key) {
      return lookedUpEntries.get(key);
   }

   public void removeLookedUpEntry(Object key) {
      lookedUpEntries.remove(key);
   }

   public void putLookedUpEntry(Object key, CacheEntry entry) {
      lookedUpEntries.put(key, entry);
   }

   public void clearLookedUpEntries() {
      lookedUpEntries.clear();
   }

   public BidirectionalMap<Object, CacheEntry> getLookedUpEntries() {
      return lookedUpEntries;
   }

   public void putLookedUpEntries(Map<Object, CacheEntry> lookedUpEntries) {
      lookedUpEntries.putAll(lookedUpEntries);
   }

   public void reset() {
      if (lookedUpEntries != null) lookedUpEntries.clear();
      flags = null;
      contextFlags = 0;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof AbstractContext)) return false;

      AbstractContext that = (AbstractContext) o;

      if (contextFlags != that.contextFlags) return false;
      if (lookedUpEntries != null ? !lookedUpEntries.equals(that.lookedUpEntries) : that.lookedUpEntries != null)
         return false;
      if (flags != null ? !flags.equals(that.flags) : that.flags != null) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = flags != null ? flags.hashCode() : 0;
      result = 31 * result + (int) contextFlags;
      result = 31 * result + (lookedUpEntries != null ? lookedUpEntries.hashCode() : 0);
      return result;
   }

   @SuppressWarnings("unchecked")
   protected void copyInto(AbstractContext ctx) {
      if (flags != null) ctx.flags = EnumSet.copyOf(flags);
      ctx.contextFlags = contextFlags;
      if (lookedUpEntries != null)
         ctx.lookedUpEntries = (BidirectionalLinkedHashMap<Object, CacheEntry>) lookedUpEntries.clone();
   }

   public boolean isContainsModifications() {
      return isContextFlagSet(ContextFlags.CONTAINS_MODS);
   }

   public void setContainsModifications(boolean b) {
      setContextFlag(ContextFlags.CONTAINS_MODS, b);
   }

   public boolean isContainsLocks() {
      return isContextFlagSet(ContextFlags.CONTAINS_LOCKS);
   }

   public void setContainsLocks(boolean b) {
      setContextFlag(ContextFlags.CONTAINS_LOCKS, b);
   }
}
