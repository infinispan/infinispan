package org.horizon.context;

import org.horizon.container.entries.CacheEntry;
import org.horizon.invocation.Options;
import org.horizon.util.BidirectionalLinkedHashMap;
import org.horizon.util.BidirectionalMap;

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

   protected volatile EnumSet<Options> options;
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

   protected final boolean isFlagSet(ContextFlags flag) {
      return (contextFlags & flag.mask) != 0;
   }

   protected final void setFlag(ContextFlags flag) {
      contextFlags |= flag.mask;
   }

   protected final void unsetFlag(ContextFlags flag) {
      contextFlags &= ~flag.mask;
   }

   protected final void setFlag(ContextFlags flag, boolean value) {
      if (value)
         setFlag(flag);
      else
         unsetFlag(flag);
   }

   public boolean hasOption(Options o) {
      return options != null && options.contains(o);
   }

   public Set<Options> getOptions() {
      return options;
   }

   public void setOptions(Options... options) {
      if (options == null || options.length == 0) return;
      if (this.options == null)
         this.options = EnumSet.copyOf(Arrays.asList(options));
      else
         this.options.addAll(Arrays.asList(options));
   }

   public void setOptions(Collection<Options> options) {
      if (options == null || options.size() == 0) return;
      if (this.options == null)
         this.options = EnumSet.copyOf(options);
      else
         this.options.addAll(options);
   }

   public void resetOptions() {
      options = null;
   }

   public boolean isOptionsUninit() {
      return options == null;
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
      options = null;
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
      if (options != null ? !options.equals(that.options) : that.options != null) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = options != null ? options.hashCode() : 0;
      result = 31 * result + (int) contextFlags;
      result = 31 * result + (lookedUpEntries != null ? lookedUpEntries.hashCode() : 0);
      return result;
   }

   @SuppressWarnings("unchecked")
   protected void copyInto(AbstractContext ctx) {
      if (options != null) ctx.options = EnumSet.copyOf(options);
      ctx.contextFlags = contextFlags;
      if (lookedUpEntries != null)
         ctx.lookedUpEntries = (BidirectionalLinkedHashMap<Object, CacheEntry>) lookedUpEntries.clone();
   }

   public boolean isContainsModifications() {
      return isFlagSet(ContextFlags.CONTAINS_MODS);
   }

   public void setContainsModifications(boolean b) {
      setFlag(ContextFlags.CONTAINS_MODS, b);
   }

   public boolean isContainsLocks() {
      return isFlagSet(ContextFlags.CONTAINS_LOCKS);
   }

   public void setContainsLocks(boolean b) {
      setFlag(ContextFlags.CONTAINS_LOCKS, b);
   }
}
