/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2011, Red Hat, Inc., and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package org.jboss.as.clustering.infinispan.subsystem;

import org.jboss.as.controller.ModelVersion;

/**
 * @author Paul Ferraro
 * @author Tristan Tarrant
 */
public enum Namespace {
    // must be first
    UNKNOWN("jboss:domain:infinispan", 0, 0),

    INFINISPAN_SERVER_6_0("infinispan:server:core", 6, 0),
    INFINISPAN_SERVER_7_0("infinispan:server:core", 7, 0),
    INFINISPAN_SERVER_7_1("infinispan:server:core", 7, 1),
    INFINISPAN_SERVER_7_2("infinispan:server:core", 7, 2),
    INFINISPAN_SERVER_8_0("infinispan:server:core", 8, 0),
    INFINISPAN_SERVER_8_1("infinispan:server:core", 8, 1),
    INFINISPAN_SERVER_8_2("infinispan:server:core", 8, 2),
    INFINISPAN_SERVER_9_0("infinispan:server:core", 9, 0),
    INFINISPAN_SERVER_9_1("infinispan:server:core", 9, 1),
    INFINISPAN_SERVER_9_2("infinispan:server:core", 9, 2);
    private static final String URN_PATTERN = "urn:%s:%d.%d";

    /**
     * The current namespace version.
     */
    public static final Namespace CURRENT = INFINISPAN_SERVER_9_2;

    private final int major;
    private final int minor;
    private final String domain;
    private final ModelVersion version;

    Namespace(String domain, int major, int minor) {
        this.domain = domain;
        this.major = major;
        this.minor = minor;
        this.version = ModelVersion.create(major, minor);
    }

    /**
     * Get the URI of this namespace.
     *
     * @return the URI
     */
    public String getUri() {
        return String.format(URN_PATTERN, domain, this.major, this.minor);
    }

    public ModelVersion getVersion() {
        return this.version;
    }

    public String format(String format) {
        return String.format(format, major, minor);
    }

    public boolean since(Namespace schema) {
        return (this.major > schema.major) || ((this.major == schema.major) && (this.minor >= schema.minor));
    }

}
