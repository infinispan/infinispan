package org.infinispan.api.mutiny;

import java.util.concurrent.TimeUnit;

import io.smallrye.mutiny.Uni;

public interface MutinyLock {

   String name();

   /**
    * Return the container of this container
    * @return
    */
   MutinyContainer container();

   Uni<Void> lock();

   Uni<Boolean> tryLock();

   Uni<Boolean> tryLock(long time, TimeUnit unit);

   Uni<Void> unlock();

   Uni<Boolean> isLocked();

   Uni<Boolean> isLockedByMe();
}
