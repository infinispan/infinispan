package org.infinispan.security.actions;

import java.security.PrivilegedAction;

/**
 * To be implemented by {@link java.security.PrivilegedAction}s which can potentially avoid costly security checks
 *
 */
public interface ContextAwarePrivilegedAction<T> extends PrivilegedAction<T> {
   boolean contextRequiresSecurity();
}
