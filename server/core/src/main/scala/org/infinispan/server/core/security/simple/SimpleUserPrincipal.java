package org.infinispan.server.core.security.simple;

import java.security.Principal;

/**
 * A principal representing an authenticated user.
 *
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
public final class SimpleUserPrincipal implements Principal {

    private final String name;

    /**
     * Construct a new instance.
     *
     * @param name the name of the user
     */
    public SimpleUserPrincipal(final String name) {
        if (name == null) {
            throw new IllegalArgumentException("name is null");
        }
        this.name = name;
    }

    /**
     * Get the name of this principal.
     *
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * Get the hash code.
     *
     * @return the hash code
     */
    public int hashCode() {
        return name.hashCode();
    }

    /**
     * Determine whether this object is equal to another.
     *
     * @param other the other object
     * @return {@code true} if they are equal, {@code false} otherwise
     */
    public boolean equals(Object other) {
        return other instanceof SimpleUserPrincipal && equals((SimpleUserPrincipal)other);
    }

    /**
     * Determine whether this object is equal to another.
     *
     * @param other the other object
     * @return {@code true} if they are equal, {@code false} otherwise
     */
    public boolean equals(SimpleUserPrincipal other) {
        return this == other || other != null && name.equals(other.name);
    }

   @Override
   public String toString() {
      return "SimpleUserPrincipal [name=" + name + "]";
   }
}
