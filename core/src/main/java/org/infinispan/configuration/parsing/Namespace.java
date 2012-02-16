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

package org.infinispan.configuration.parsing;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

import javax.xml.XMLConstants;

/**
 * An enumeration of the supported domain model namespaces.
 *
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
public enum Namespace {
    // must be first
    UNKNOWN(null), NONE(null),

    // predefined standard
    XML_SCHEMA_INSTANCE("http://www.w3.org/2001/XMLSchema-instance"),

    // configuration versions, oldest to newest
    INFINISPAN_5_0("urn:infinispan:config:5.0"),
    INFINISPAN_5_1("urn:infinispan:config:5.1"),
    INFINISPAN_5_2("urn:infinispan:config:5.2");

    /**
     * The current namespace version.
     */
    public static final Namespace CURRENT = INFINISPAN_5_2;

    private static final Namespace[] SUPPORTED_NAMESPACES = new Namespace[] {
          INFINISPAN_5_2, INFINISPAN_5_1, INFINISPAN_5_0, NONE};

    public boolean isSupported() {
       for (Namespace ns : SUPPORTED_NAMESPACES) if (this == ns) return true;
       return false;
    }

    private final String name;

    Namespace(final String name) {
        this.name = name;
    }

    /**
     * Get the URI of this namespace.
     *
     * @return the URI
     */
    public String getUriString() {
        return name;
    }

    /**
     * Set of all namespaces, excluding the special {@link #UNKNOWN} value.
     */
    public static final EnumSet<Namespace> STANDARD_NAMESPACES = EnumSet.complementOf(EnumSet.of(UNKNOWN, XML_SCHEMA_INSTANCE));

    private static final Map<String, Namespace> MAP;

    static {
        final Map<String, Namespace> map = new HashMap<String, Namespace>();
        for (Namespace namespace : values()) {
            final String name = namespace.getUriString();
            if (name != null)
                map.put(name, namespace);
        }
        MAP = map;
    }

    public static Namespace forUri(String uri) {
        // FIXME when STXM-8 is done, remove the null check
        if (uri == null || XMLConstants.NULL_NS_URI.equals(uri))
            return NONE;
        final Namespace element = MAP.get(uri);
        return element == null ? UNKNOWN : element;
    }
}
