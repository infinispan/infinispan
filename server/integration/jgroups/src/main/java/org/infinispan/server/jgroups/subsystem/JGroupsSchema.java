/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2014, Red Hat, Inc., and individual contributors
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
package org.infinispan.server.jgroups.subsystem;

/**
 * Enumeration of the supported subsystem xml schemas.
 * @author Paul Ferraro
 */
enum JGroupsSchema {

    INFINISPAN_SERVER_JGROUPS_7_0("infinispan:server:jgroups", 7, 0),
    INFINISPAN_SERVER_JGROUPS_8_0("infinispan:server:jgroups", 8, 0),
    INFINISPAN_SERVER_JGROUPS_9_0("infinispan:server:jgroups", 9, 0),
    INFINISPAN_SERVER_JGROUPS_9_2("infinispan:server:jgroups", 9, 2),
    INFINISPAN_SERVER_JGROUPS_9_3("infinispan:server:jgroups", 9, 3),
    ;
    public static final JGroupsSchema CURRENT = INFINISPAN_SERVER_JGROUPS_9_3;

    private static final String URN_PATTERN = "urn:%s:%d.%d";

    private final String domain;
    private final int major;
    private final int minor;

    JGroupsSchema(String domain, int major, int minor) {
        this.domain = domain;
        this.major = major;
        this.minor = minor;
    }

    /**
     * Indicates whether this version of the schema is greater than or equal to the version of the specified schema.
     * @param a schema
     * @return true, if this version of the schema is greater than or equal to the version of the specified schema, false otherwise.
     */
    public boolean since(JGroupsSchema... schemas) {
        for(JGroupsSchema schema : schemas) {
            if ((this.domain.equals(schema.domain) && ((this.major > schema.major) || ((this.major == schema.major) && (this.minor >= schema.minor)))))
                return true;
        }
        return false;
    }

    /**
     * Get the namespace URI for this schema version.
     * @return the namespace URI
     */
    public String getNamespaceUri() {
        return this.format(URN_PATTERN);
    }

    /**
     * Formats a string using the specified pattern.
     * @param pattern a formatter pattern
     * @return a formatted string
     */
    String format(String pattern) {
        return String.format(pattern, this.domain, this.major, this.minor);
    }
}
