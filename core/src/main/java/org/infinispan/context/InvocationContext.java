package org.infinispan.context;

import java.util.Collection;
import java.util.Set;

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
    * Tracks the given key as locked by this invocation context.
    */
   void addLockedKey(Object key);

   default void addLockedKeys(Collection<?> keys) {
      for (Object key : keys) {
         addLockedKey(key);
      }
   }

   /**
    * Returns true if the lock being tested is already held in the current scope, false otherwise.
    *
    * @param key lock to test
    */
   boolean hasLockedKey(Object key);

   /**
    * @deprecated Since 11, to be removed in 14 with no replacement
    */
   boolean isEntryRemovedInContext(Object key);
}
