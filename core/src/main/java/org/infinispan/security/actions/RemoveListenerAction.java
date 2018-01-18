package org.infinispan.security.actions;

import org.infinispan.notifications.Listenable;

/**
 * RemoveListenerAction
 *
 * @author vjuranek
 * @since 9.0
 */
public class RemoveListenerAction implements ContextAwarePrivilegedAction<Void> {

    private final Listenable listenable;
    private final Object listener;

    public RemoveListenerAction(Listenable listenable, Object listener) {
        this.listenable = listenable;
        this.listener = listener;
    }

    @Override
    public Void run() {
        listenable.removeListener(listener);
        return null;
    }

    @Override
    public boolean contextRequiresSecurity() {
        return true; // err on the safe side
    }
}
