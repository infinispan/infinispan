package org.infinispan.cdi.common.util;

import java.lang.reflect.AccessibleObject;
import java.security.PrivilegedAction;

/**
 * A {@link PrivilegedAction} that calls
 * {@link AccessibleObject#setAccessible(boolean)}
 *
 * @author Stuart Douglas
 */
public class SetAccessiblePrivilegedAction implements PrivilegedAction<Void> {

    private final AccessibleObject member;

    public SetAccessiblePrivilegedAction(AccessibleObject member) {
        this.member = member;
    }

    public Void run() {
        member.setAccessible(true);
        return null;
    }

}
