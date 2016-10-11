package org.infinispan.util.concurrent.locks;

import java.util.Collection;

/**
 * Simple interface to extract all the keys that may need to be locked.
 * <p>
 * A {@link org.infinispan.commands.remote.CacheRpcCommand} that needs to acquire locks should implement this interface.
 * This way, Infinispan tries to provide a better management to optimize the system resources usage.
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public interface RemoteLockCommand {

   /**
    * It returns a {@link Collection} with the keys to be lock.
    * <p>
    * It may return an empty collection if no keys needs to be locked independently of the return value of {@link
    * #hasSkipLocking()}. It may contains duplicated keys and {@code null} is not a valid return value.
    *
    * @return a {@link Collection} of keys to lock.
    */
   Collection<?> getKeysToLock();

   /**
    * It returns the lock owner of the key.
    * <p>
    * Usually, in transaction caches it is the {@link org.infinispan.transaction.xa.GlobalTransaction} and in
    * non-transactional caches the {@link org.infinispan.commands.CommandInvocationId}.
    *
    * @return the lock owner of the key.
    */
   Object getKeyLockOwner();

   /**
    * @return it the locks should be acquire with 0 (zero) acquisition timeout.
    */
   boolean hasZeroLockAcquisition();

   /**
    * It checks if this command should acquire locks.
    *
    * @return {@code true} if locks should be acquired for the keys in {@link #getKeysToLock()}.
    */
   boolean hasSkipLocking();
}
