package org.infinispan.context;

import java.util.Set;
import java.util.concurrent.CompletionStage;

import org.infinispan.container.impl.EntryFactory;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.remoting.transport.Address;

/**
 * A context that contains information pertaining to a given invocation.  These contexts typically have the lifespan of
 * a single invocation.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public interface InvocationContext extends EntryLookup, Cloneable {

   /**
    * Returns true if the call was originated locally, false if it is the result of a remote rpc.
    */
   boolean isOriginLocal();

   /**
    * @return the origin of the command, or null if the command originated locally
    */
   Address getOrigin();

   /**
    * Returns true if this call is performed in the context of an transaction, false otherwise.
    */
   boolean isInTxScope();

   /**
    * Returns the in behalf of which locks will be acquired.
    */
   Object getLockOwner();

   /**
    * Sets the object to be used by lock owner.
    */
   void setLockOwner(Object lockOwner);

   /**
    * Clones the invocation context.
    *
    * @return A cloned instance of this invocation context instance
    */
   InvocationContext clone();

   /**
    * Returns the set of keys that are locked for writing.
    */
   Set<Object> getLockedKeys();

   void clearLockedKeys();

   /**
    * Returns the class loader associated with this invocation
    *
    * @return a class loader instance or null if no class loader was
    *         specifically associated
    * @deprecated Not in use any more, implementations might return null.
    */
   @Deprecated
   ClassLoader getClassLoader();

   /**
    * Sets the class loader associated for this invocation
    *
    * @deprecated Not in use any more.
    */
   @Deprecated
   void setClassLoader(ClassLoader classLoader);

   /**
    * Tracks the given key as locked by this invocation context.
    */
   void addLockedKey(Object key);

   /**
    * Returns true if the lock being tested is already held in the current scope, false otherwise.
    *
    * @param key lock to test
    */
   boolean hasLockedKey(Object key);

   /**
    * @deprecated Since 8.1, use {@link EntryFactory#wrapExternalEntry(InvocationContext, Object, CacheEntry, boolean, boolean)} instead.
    */
   @Deprecated
   default boolean replaceValue(Object key, InternalCacheEntry cacheEntry) {
      return false;
   }

   boolean isEntryRemovedInContext(Object key);

   /**
    * Rationale: Commands can fork processing to multiple threads, e.g. by issuing an async RPC
    * and adding a handler to that. In some cases involving exceptions the handlers might be invoked
    * in parallel. In order to not synchronize all collections in the context and prevent inconsistencies
    * we introduce a big lock on the context.
    *
    * Acquires a non-reentrant lock on the context. This must be called before processing command
    * with this context.
    * @return Null if the code is free to continue or a stage if the code should be blocked.
    *         Any handler registered on this stage will be executed with acquired lock.
    * @throws IllegalStateException If the implementation is not expecting any concurrent access
    *         but we observe that. Example implementation could be a CAS returning non-zero.
    */
   CompletionStage<Void> enter();

   /**
    * Releases a lock on the context. This must be called before releasing the control to other
    * threads, e.g. after a RPC has been invoked and there's no more processing to be done by this thread.
    */
   void exit();
}
