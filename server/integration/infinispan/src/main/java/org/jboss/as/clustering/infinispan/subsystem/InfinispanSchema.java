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
package org.jboss.as.clustering.infinispan.subsystem;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.jboss.as.controller.ModelVersion;

/**
 * Holds the supported subsystem xml schemas.
 */
class InfinispanSchema {
    private static final String URN_PATTERN = "urn:%s:%d.%d";

    static final List<InfinispanSchema> SCHEMAS;
    static final InfinispanSchema CURRENT;
    static {
        SCHEMAS = new ArrayList<>();
        try (BufferedReader r = new BufferedReader(new InputStreamReader(InfinispanSchema.class.getResourceAsStream("/schema/infinispan-infinispan.namespaces"), StandardCharsets.UTF_8))) {
            r.lines().forEach(line -> {
                int colon = line.lastIndexOf(':');
                String parts[] = line.substring(colon + 1).split("\\.");
                SCHEMAS.add(new InfinispanSchema(line.substring(0, colon), Integer.parseInt(parts[0]), Integer.parseInt(parts[1])));
            });
            CURRENT = SCHEMAS.get(SCHEMAS.size() - 1);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private final String domain;
    private final int major;
    private final int minor;

    private InfinispanSchema(String domain, int major, int minor) {
        this.domain = domain;
        this.major = major;
        this.minor = minor;
    }

    public boolean since(int major, int minor) {
        return (this.major > major || (this.major == major && this.minor >= minor));
    }

    public boolean equals(int major, int minor) {
        return this.major == major && this.minor == minor;
    }

    /**
     * @return true, if this version of the schema is greater than or equal to the version of any of the specified schemas, false otherwise.
     */
    public boolean since(InfinispanSchema... schemas) {
        for(InfinispanSchema schema : schemas) {
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

    ModelVersion getVersion() {
        return ModelVersion.create(major, minor);
    }
}
