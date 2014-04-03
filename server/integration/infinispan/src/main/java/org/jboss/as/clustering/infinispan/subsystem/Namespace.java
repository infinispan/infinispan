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

import java.util.List;

import org.jboss.dmr.ModelNode;
import org.jboss.staxmapper.XMLElementReader;

/**
 * @author Paul Ferraro
 * @author Tristan Tarrant
 */
public enum Namespace {
    // must be first
    UNKNOWN("jboss:domain:infinispan", 0, 0, null),

    INFINISPAN_1_0("jboss:domain:infinispan", 1, 0, new InfinispanSubsystemXMLReader_1_0()),
    INFINISPAN_1_1("jboss:domain:infinispan", 1, 1, new InfinispanSubsystemXMLReader_1_1()),
    INFINISPAN_1_2("jboss:domain:infinispan", 1, 2, new InfinispanSubsystemXMLReader_1_2()), // IMPORTANT: Management API version != XSD version!
    INFINISPAN_1_3("jboss:domain:infinispan", 1, 3, new InfinispanSubsystemXMLReader_1_3()), // IMPORTANT: Management API version != XSD version!
    INFINISPAN_1_4("jboss:domain:infinispan", 1, 4, new InfinispanSubsystemXMLReader_1_4()), // IMPORTANT: Management API version != XSD version!
    INFINISPAN_SERVER_5_2("infinispan:server:core", 5, 2, new InfinispanSubsystemXMLReader_5_2()),
    INFINISPAN_SERVER_5_3("infinispan:server:core", 5, 3, new InfinispanSubsystemXMLReader_5_3()),
    INFINISPAN_SERVER_6_0("infinispan:server:core", 6, 0, new InfinispanSubsystemXMLReader_6_0()),
    INFINISPAN_SERVER_7_0("infinispan:server:core", 7, 0, new InfinispanSubsystemXMLReader_7_0()),
    ;
    private static final String URN_PATTERN = "urn:%s:%d.%d";

    /**
     * The current namespace version.
     */
    public static final Namespace CURRENT = INFINISPAN_SERVER_7_0;

    private final int major;
    private final int minor;
    private final XMLElementReader<List<ModelNode>> reader;
    private final String domain;

    Namespace(String domain, int major, int minor, XMLElementReader<List<ModelNode>> reader) {
        this.domain = domain;
        this.major = major;
        this.minor = minor;
        this.reader = reader;
    }

    /**
     * Get the URI of this namespace.
     *
     * @return the URI
     */
    public String getUri() {
        return String.format(URN_PATTERN, domain, this.major, this.minor);
    }

    public XMLElementReader<List<ModelNode>> getXMLReader() {
        return this.reader;
    }
}
