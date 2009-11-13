package org.infinispan.context.impl;

import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.util.BidirectionalMap;

import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Set;

/**
 * Common features of transaction and invocation contexts
 *
 * @author Manik Surtani
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public abstract class AbstractInvocationContext implements InvocationContext {

   protected volatile EnumSet<Flag> flags;

   // since this is finite, small, and strictly an internal API, it is cheaper/quicker to use bitmasking rather than
   // an EnumSet.
   protected byte contextFlags = 0;

   // if this or any context subclass ever needs to store a boolean, always use a context flag instead.  This is far
   // more space-efficient.  Note that this value will be stored in a byte, which means up to 8 flags can be stored in
   // a single byte.  Always start shifting with 0, the last shift cannot be greater than 7.
   protected enum ContextFlag {
      USE_FUTURE_RETURN_TYPE(1), // same as 1 << 0
      ORIGIN_LOCAL(1 << 1);

      final byte mask;

      ContextFlag(int mask) {
         this.mask = (byte) mask;
      }
   }

   /**
    * Tests whether a context flag is set.
    *
    * @param flag context flag to test
    * @return true if set, false otherwise.
    */
   protected final boolean isContextFlagSet(ContextFlag flag) {
      return (contextFlags & flag.mask) != 0;
   }

   /**
    * Unility method that sets a given context flag.
    *
    * @param flag context flag to set
    */
   protected final void setContextFlag(ContextFlag flag) {
      contextFlags |= flag.mask;
   }

   /**
    * Utility method that unsets a context flag.
    *
    * @param flag context flag to unset
    */
   protected final void unsetContextFlag(ContextFlag flag) {
      contextFlags &= ~flag.mask;
   }

   /**
    * Utility value that sets or unsets a context flag based on a boolean passed in
    *
    * @param flag flag to set or unset
    * @param set  if true, the context flag is set.  If false, the context flag is unset.
    */
   protected final void setContextFlag(ContextFlag flag, boolean set) {
      if (set)
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

   public void reset() {
      flags = null;
      contextFlags = 0;
   }

   public boolean isFlagsUninitialized() {
      return flags == null;
   }

   public boolean hasLockedKey(Object key) {
      CacheEntry e = lookupEntry(key);
      if (e == null) {
         return getLookedUpEntries().containsKey(key); // this will chk if the key is present
      } else {
         return e.isChanged();
      }
   }

   public boolean hasLockedEntries() {
      BidirectionalMap<Object, CacheEntry> lookedUpEntries = getLookedUpEntries();
      boolean result = false;
      for (CacheEntry e : lookedUpEntries.values()) {
         if (e.isChanged()) {
            result = true;
         }
      }
      return result;
   }


   public boolean isUseFutureReturnType() {
      return isContextFlagSet(ContextFlag.USE_FUTURE_RETURN_TYPE);
   }

   public void setUseFutureReturnType(boolean useFutureReturnType) {
      setContextFlag(ContextFlag.USE_FUTURE_RETURN_TYPE, useFutureReturnType);
   }

   @Override
   public AbstractInvocationContext clone() {
      try {
         AbstractInvocationContext dolly = (AbstractInvocationContext) super.clone();
         if (flags != null) dolly.flags = flags.clone();
         return dolly;
      } catch (CloneNotSupportedException e) {
         throw new IllegalStateException("Impossible!");
      }
   }

   @Override
   public String toString() {
      return getClass().getSimpleName() + "{" +
            "flags=" + flags +
            '}';
   }
}
