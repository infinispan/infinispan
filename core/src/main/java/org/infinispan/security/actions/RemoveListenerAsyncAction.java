package org.infinispan.security.actions;

import java.security.PrivilegedAction;
import java.util.concurrent.CompletionStage;

import org.infinispan.notifications.Listenable;

/**
 * removeListenerAsync action
 *
 * @author Dan Berindei
 * @since 13.0
 */
public class RemoveListenerAsyncAction implements PrivilegedAction<CompletionStage<Void>> {

    private final Listenable listenable;
    private final Object listener;

    public RemoveListenerAsyncAction(Listenable listenable, Object listener) {
        this.listenable = listenable;
        this.listener = listener;
    }

    @Override
    public CompletionStage<Void> run() {
        return listenable.removeListenerAsync(listener);
    }

}
