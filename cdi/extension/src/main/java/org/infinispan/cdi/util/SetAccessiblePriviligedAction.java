package org.infinispan.cdi.util;

import java.lang.reflect.AccessibleObject;
import java.security.PrivilegedAction;

/**
 * A {@link PrivilegedAction} that calls
 * {@link AccessibleObject#setAccessible(boolean)}
 *
 * @author Stuart Douglas
 */
public class SetAccessiblePriviligedAction implements PrivilegedAction<Void> {

    private final AccessibleObject member;

    public SetAccessiblePriviligedAction(AccessibleObject member) {
        this.member = member;
    }

    public Void run() {
        member.setAccessible(true);
        return null;
    }

}
