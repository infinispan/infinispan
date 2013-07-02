package org.infinispan.transaction;

/**
 * Defines the locking modes that are available for transactional caches:
 * <a href="http://community.jboss.org/wiki/OptimisticLockingInInfinispan"></a>optimistic</a> or pessimistic.
 *
 * @author Mircea Markus
 * @since 5.1
 */
public enum LockingMode {
   OPTIMISTIC, PESSIMISTIC
}
