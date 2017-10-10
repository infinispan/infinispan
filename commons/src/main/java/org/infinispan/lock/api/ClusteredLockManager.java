package org.infinispan.lock.api;

import java.util.concurrent.CompletableFuture;

import org.infinispan.commons.util.Experimental;
import org.infinispan.lock.exception.ClusteredLockException;

/**
 * Provides the API to define, create and remove ClusteredLocks.
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 9.2
 */
@Experimental
public interface ClusteredLockManager {

   /**
    * Defines a lock with the specific name and the default {@link ClusteredLockConfiguration}. It does not overwrite
    * existing configurations. Returns true if successfully defined or false if the lock is already defined or any other
    * failure.
    *
    * @param name, the name of the lock
    * @return true if the lock was successfully defined
    */
   boolean defineLock(String name);

   /**
    * Defines a lock with the specific name and {@link ClusteredLockConfiguration}. It does not overwrite existing
    * configurations. Returns true if successfully defined or false if the lock is already defined or any other
    * failure.
    *
    * @param name,          the name of the lock
    * @param configuration, a {@link ClusteredLockConfiguration} object with the configuration of the lock
    * @return true if the lock was successfully defined
    */
   boolean defineLock(String name, ClusteredLockConfiguration configuration);

   /**
    * Get’s a {@link ClusteredLock} by it’s name. This method throws {@link ClusteredLockException} if the lock is not
    * not defined. A call of {@link #defineLock} must be done at least once in the cluster. This method will return the
    * same lock object depending on the {@link OwnershipLevel}.
    *
    * If the {@link OwnershipLevel} is {@link OwnershipLevel#NODE}, it wll return the same instance per {@link ClusteredLockManager}
    * If the {@link OwnershipLevel} is {@link OwnershipLevel#INSTANCE}, it wll return a new instance per call.
    *
    * @param name, the name of the lock
    * @return {@link ClusteredLock} instance
    * @throws ClusteredLockException, when the lock is not defined
    */
   ClusteredLock get(String name);

   /**
    * Returns the configuration of a {@link ClusteredLock}, if such exists.This method throws {@link
    * ClusteredLockException} if the lock is not not defined. A call of {@link #defineLock} must be done at least once
    * in the cluster.
    *
    * @param name, the name of the lock
    * @return {@link ClusteredLockConfiguration} for this lock
    * @throws ClusteredLockException, when the lock is not defined
    */
   ClusteredLockConfiguration getConfiguration(String name);

   /**
    * Checks if a lock is already defined.
    *
    * @param name, the lock name
    * @return {@code true} if this lock is defined
    */
   boolean isDefined(String name);

   /**
    * Removes a {@link ClusteredLock} if such exists.
    *
    * @param name, the name of the lock
    * @return {@code true} if the lock is removed
    */
   CompletableFuture<Boolean> remove(String name);

   /**
    * Releases - or unlocks - a {@link ClusteredLock} if such exists.
    * This method is used when we just want to force the release the lock no matter who is holding it at a given time.
    * Calling this method may cause concurrency issues and has to be used in exceptional situations.
    *
    * @param name, the name of the lock
    * @return {@code true} if the lock has been released
    */
   CompletableFuture<Boolean> forceRelease(String name);
}
