package org.infinispan.api.async;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

/**
 * @since 14.0
 */
public interface AsyncLock {
   String name();

   /**
    * Return the container of this lock
    * @return
    */
   AsyncContainer container();

   CompletionStage<Void> lock();

   CompletionStage<Boolean> tryLock();

   CompletionStage<Boolean> tryLock(long time, TimeUnit unit);

   CompletionStage<Void> unlock();

   CompletionStage<Boolean> isLocked();

   CompletionStage<Boolean> isLockedByMe();
}
