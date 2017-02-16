package org.infinispan.commands;

import java.util.UUID;

import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

/**
 * CancellationService manages association of Thread executing CancellableCommand in a remote VM and
 * if needed cancels command execution
 *
 * @author Vladimir Blagojevic
 * @since 5.2
 */
@Scope(Scopes.GLOBAL)
public interface CancellationService {

   /**
    * Registers thread with {@link CancellationService} under the given UUID id
    *
    * @param t
    *           thread to associate with id
    * @param id
    *           chosen UUID id
    */
   public void register(Thread t, UUID id);

   /**
    * Unregisters thread with {@link CancellationService} given an id
    *
    * @param id
    *           thread id
    */
   public void unregister(UUID id);

   /**
    * Cancels (invokes Thread#interrupt) a thread given a thread id
    *
    * @param id
    *           thread id
    */
   public void cancel(UUID id);

}
