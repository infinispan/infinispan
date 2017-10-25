package org.infinispan.util.concurrent.locks;

import java.util.function.Supplier;

import org.infinispan.interceptors.InvocationStage;
import org.infinispan.util.concurrent.TimeoutException;

/**
 * An extended {@link LockPromise} interface that allows a better control over it.
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public interface ExtendedLockPromise extends LockPromise {

   /**
    * It cancels the {@link LockPromise} if possible.
    *
    * @param cause the cancellation cause. The possible values are {@link LockState#DEADLOCKED} and {@link
    *              LockState#TIMED_OUT}.
    * @throws IllegalArgumentException if the argument {@code cause} is not valid.
    */
   void cancel(LockState cause);

   /**
    * @return the lock owner associated to this {@link LockPromise}.
    */
   Object getRequestor();

   /**
    * @return the current lock owner.
    */
   Object getOwner();

   /**
    * @return an {@link InvocationStage} for this lock.
    */
   InvocationStage toInvocationStage(Supplier<TimeoutException> timeoutSupplier);
}
