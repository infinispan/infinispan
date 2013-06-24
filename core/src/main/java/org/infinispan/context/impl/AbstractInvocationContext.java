package org.infinispan.context.impl;

import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.remoting.transport.Address;

import java.lang.ref.WeakReference;

/**
 * Common features of transaction and invocation contexts
 *
 * @author Manik Surtani
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public abstract class AbstractInvocationContext implements InvocationContext {

   // since this is finite, small, and strictly an internal API, it is cheaper/quicker to use bitmasking rather than
   // an EnumSet.
   protected byte contextFlags = 0;
   private Address origin;
   // Class loader associated with this invocation which supports AdvancedCache.with() functionality
   private WeakReference<ClassLoader> classLoader;

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
    * Utility method that sets a given context flag.
    *
    * @param flag context flag to set
    */
   protected final void setContextFlag(ContextFlag flag) {
      contextFlags |= flag.mask;
   }

   /**
    * Utility method that un-sets a context flag.
    *
    * @param flag context flag to unset
    */
   protected final void unsetContextFlag(ContextFlag flag) {
      contextFlags &= ~flag.mask;
   }

   /**
    * Utility value that sets or un-sets a context flag based on a boolean passed in
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

   @Override
   public Address getOrigin() {
	   return origin;
   }
   
   public void setOrigin(Address origin) {
	   this.origin = origin;
   }

   @Override
   public boolean hasLockedKey(Object key) {
      return getLockedKeys().contains(key);
   }


   @Override
   public boolean isUseFutureReturnType() {
      return isContextFlagSet(ContextFlag.USE_FUTURE_RETURN_TYPE);
   }

   @Override
   public void setUseFutureReturnType(boolean useFutureReturnType) {
      setContextFlag(ContextFlag.USE_FUTURE_RETURN_TYPE, useFutureReturnType);
   }

   @Override
   public AbstractInvocationContext clone() {
      try {
         return (AbstractInvocationContext) super.clone();
      } catch (CloneNotSupportedException e) {
         throw new IllegalStateException("Impossible!");
      }
   }

   @Override
   public ClassLoader getClassLoader() {
      return classLoader != null ? classLoader.get() : null;
   }

   @Override
   public void setClassLoader(ClassLoader classLoader) {
      this.classLoader = new WeakReference<ClassLoader>(classLoader);
   }

   @Override
   public boolean replaceValue(Object key, InternalCacheEntry cacheEntry) {
      CacheEntry ce = lookupEntry(key);
      if (ce == null || ce.isNull() || ce.getValue() == null) {
         if (ce != null) {
            ce.setValue(cacheEntry.getValue());
            ce.setMetadata(cacheEntry.getMetadata());
            onEntryValueReplaced(key, cacheEntry);
         } else {
            return false;
         }
      }
      return true;
   }

   @Override
   public boolean isEntryRemovedInContext(Object key) {
      CacheEntry ce = lookupEntry(key);
      return ce != null && ce.isRemoved() && ce.isChanged();
   }

   protected void onEntryValueReplaced(Object key, InternalCacheEntry cacheEntry) {
      //no-op. used in tx mode with write skew check.
   }
}
