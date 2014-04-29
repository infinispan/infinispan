/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009, JBoss Inc., and individual contributors as indicated
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.infinispan.server.endpoint.subsystem;

import javax.security.sasl.Sasl;

/**
 * The SASL quality-of-protection value.
 *
 * @see Sasl#QOP
 */
public enum SaslQop {

    /**
     * A QOP value specifying authentication only.
     */
    AUTH("auth"),
    /**
     * A QOP value specifying authentication plus integrity protection.
     */
    AUTH_INT("auth-int"),
    /**
     * A QOP value specifying authentication plus integrity and confidentiality protection.
     */
    AUTH_CONF("auth-conf"),
    ;

    private final String s;

    SaslQop(String s) {
        this.s = s;
    }

    /**
     * Get the SASL QOP level for the given string.
     *
     * @param name the QOP level
     * @return the QOP value
     */
    public static SaslQop fromString(String name) {
        if ("auth".equals(name)) {
            return AUTH;
        } else if ("auth-int".equals(name)) {
            return AUTH_INT;
        } else if ("auth-conf".equals(name)) {
            return AUTH_CONF;
        } else {
            throw new IllegalArgumentException("Invalid QOP string given");
        }
    }

    /**
     * Get the string representation of this SASL QOP value.
     *
     * @return the string representation
     */
    public String getString() {
        return s;
    }

    /**
     * Get the human-readable string representation of this SASL QOP value.
     *
     * @return the string representation
     */
    public String toString() {
        return s;
    }
}
