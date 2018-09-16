package org.infinispan.remoting.inboundhandler.action;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.commons.util.InfinispanCollections;

/**
 * A list of {@link Action} to be executed to check when it is ready.
 * <p>
 * If an {@link Action} is canceled, then the remaining {@link Action} are not invoked.
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public class DefaultReadyAction implements ReadyAction, ActionListener {

   private final ActionState state;
   private final Action[] actions;
   private final AtomicInteger currentAction;
   private final CompletableFuture<Void> notifier;

   public DefaultReadyAction(ActionState state, Action... actions) {
      this.state = Objects.requireNonNull(state, "Action state must be non null.");
      this.actions = Objects.requireNonNull(actions, "Actions must be non null.");
      notifier = new CompletableFuture<>();
      currentAction = new AtomicInteger(0);
   }

   public void registerListener() {
      InfinispanCollections.forEach(actions, action -> action.addListener(this));
   }

   @Override
   public boolean isReady() {
      int current = currentAction.get();
      if (current >= actions.length) {
         return true;
      }
      Action action = actions[current];
      switch (action.check(state)) {
         case READY:
            //check the next action. If currentAction has changed, some thread already advanced.
            return currentAction.compareAndSet(current, current + 1) && isReady();
         case NOT_READY:
            return false;
         case CANCELED:
            currentAction.set(actions.length);
            return true;
      }
      return false;
   }

   @Override
   public void addListener(ActionListener listener) {
      notifier.thenRun(listener::onComplete);
   }

   @Override
   public void onException() {
      InfinispanCollections.forEach(actions, action -> action.onException(state));
   }

   @Override
   public void onComplete() {
      if (isReady()) {
         notifier.complete(null);
      }
   }

   @Override
   public void onFinally() {
      InfinispanCollections.forEach(actions, action -> action.onFinally(state));
   }
}
