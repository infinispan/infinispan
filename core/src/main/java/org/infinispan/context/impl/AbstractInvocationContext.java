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

   public boolean hasLockedKey(Object key) {
      CacheEntry e = lookupEntry(key);
      return e != null && e.isChanged();
   }

   public boolean hasLockedEntries() {
      BidirectionalMap<Object, CacheEntry> lookedUpEntries = getLookedUpEntries();
      boolean result = false;
      for (CacheEntry e : lookedUpEntries.values()) {
         if (e.isChanged()) {
            System.out.println("Entry is locked = " + e);
            result = true;
         }
      }
      return result;
   }


   @Override
   public Object clone() {
      try {
         AbstractInvocationContext dolly = (AbstractInvocationContext) super.clone();
         if (flags != null)
            dolly.flags = flags.clone();
         return dolly;
      } catch (CloneNotSupportedException e) {
         throw new IllegalStateException("Imposible!");
      }
   }
}
