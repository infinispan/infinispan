package org.infinispan.xsite.statetransfer;

import java.util.concurrent.CompletionStage;

import io.reactivex.rxjava3.core.Flowable;

/**
 * Sends local cluster state to remote site.
 *
 * @author Pedro Ruivo
 * @since 12.0
 */
public interface XSiteStatePushTask {

   /**
    * Perform the state transfer with the state from {@link Flowable}.
    * <p>
    * The {@link Flowable} can only be iterated after {@code delayer} is completed.
    *
    * @param flowable The {@link Flowable} with the local cluster state.
    * @param delayer  A {@link CompletionStage} which is completed when it is allowed to start sending the state.
    */
   void execute(Flowable<XSiteState> flowable, CompletionStage<Void> delayer);
}
