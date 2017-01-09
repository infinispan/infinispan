package org.infinispan.remoting.inboundhandler.action;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A {@link ReadyAction} implementation that delegates it logic to a collection of other {@link ReadyAction}.
 *
 * @author Pedro Ruivo
 * @since 8.1
 */
public class CompositeAction implements ReadyAction, ActionListener {

   private final Collection<ReadyAction> actions;
   private final AtomicBoolean notify;
   private volatile ActionListener listener;

   public CompositeAction(Collection<ReadyAction> actions) {
      this.actions = actions;
      notify = new AtomicBoolean(false);
   }

   public void registerListener() {
      actions.forEach(readyAction -> readyAction.addListener(this));
   }

   @Override
   public boolean isReady() {
      for (ReadyAction action : actions) {
         if (!action.isReady()) {
            return false;
         }
      }
      return true;
   }

   @Override
   public void addListener(ActionListener listener) {
      this.listener = listener;
   }

   @Override
   public void onException() {
      actions.forEach(ReadyAction::onException);
   }

   @Override
   public void onFinally() {
      actions.forEach(ReadyAction::onFinally);
   }

   @Override
   public void onComplete() {
      ActionListener actionListener = listener;
      if (isReady() && actionListener != null && notify.compareAndSet(false, true)) {
         actionListener.onComplete();
      }
   }
}
