/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011-2013 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
package org.infinispan.server.endpoint.subsystem;

import org.jboss.as.controller.ModelVersion;

/**
 * @author Tristan Tarrant
 */
public enum Namespace {
    // must be first
    UNKNOWN(null, 0, 0),

    INFINISPAN_ENDPOINT_7_2("infinispan:server:endpoint", 7, 2),
    INFINISPAN_ENDPOINT_8_0("infinispan:server:endpoint", 8, 0),
    INFINISPAN_ENDPOINT_9_0("infinispan:server:endpoint", 9, 0),
    INFINISPAN_ENDPOINT_9_2("infinispan:server:endpoint", 9, 2),
    INFINISPAN_ENDPOINT_9_3("infinispan:server:endpoint", 9, 3),
    INFINISPAN_ENDPOINT_9_4("infinispan:server:endpoint", 9, 4),
    ;
    private static final String URN_PATTERN = "urn:%s:%d.%d";

    /**
     * The current namespace version.
     */
    public static final Namespace CURRENT = INFINISPAN_ENDPOINT_9_4;

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
