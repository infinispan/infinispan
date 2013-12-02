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

import java.util.List;

import org.jboss.dmr.ModelNode;
import org.jboss.staxmapper.XMLElementReader;

/**
 * @author Tristan Tarrant
 */
public enum Namespace {
    // must be first
    UNKNOWN(null, 0, 0, null),

    INFINISPAN_ENDPOINT_1_0("jboss:domain:datagrid", 1, 0, new EndpointSubsystemReader_1_0()),
    INFINISPAN_ENDPOINT_5_2("infinispan:server:endpoint", 5, 2, new EndpointSubsystemReader_1_0()),
    INFINISPAN_ENDPOINT_5_3("infinispan:server:endpoint", 5, 3, new EndpointSubsystemReader_5_3()),
    INFINISPAN_ENDPOINT_6_0("infinispan:server:endpoint", 6, 0, new EndpointSubsystemReader_6_0()),
    ;
    private static final String URN_PATTERN = "urn:%s:%d.%d";

    /**
     * The current namespace version.
     */
    public static final Namespace CURRENT = INFINISPAN_ENDPOINT_6_0;

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
