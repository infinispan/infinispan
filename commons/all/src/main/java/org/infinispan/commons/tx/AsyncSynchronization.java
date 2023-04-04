package org.infinispan.commons.tx;

import java.util.concurrent.CompletionStage;

import jakarta.transaction.Synchronization;

/**
 * Non-blocking {@link javax.transaction.Synchronization}.
 *
 * @since 14.0
 */
public interface AsyncSynchronization {

   /**
    * @return A {@link CompletionStage} which is completed with the result of {@link Synchronization#beforeCompletion()}.
    * @see Synchronization#beforeCompletion()
    */
   CompletionStage<Void> asyncBeforeCompletion();

   /**
    * @return A {@link CompletionStage} which is completed with the result of {@link Synchronization#afterCompletion(int)}.
    * @see Synchronization#afterCompletion(int)
    */
   CompletionStage<Void> asyncAfterCompletion(int status);

}
