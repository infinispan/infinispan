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

import java.util.HashMap;
import java.util.Map;

import org.jboss.as.controller.AttributeDefinition;

/**
 * Enumerates the elements used in the Infinispan Endpoint subsystem schema.

 * @author Tristan Tarrant
 * @since 5.3
 */
public enum Element {
    // must be first
    UNKNOWN((String)null),

    HOTROD_CONNECTOR(ModelKeys.HOTROD_CONNECTOR),
    MEMCACHED_CONNECTOR(ModelKeys.MEMCACHED_CONNECTOR),
    REST_CONNECTOR(ModelKeys.REST_CONNECTOR),
    WEBSOCKET_CONNECTOR(ModelKeys.WEBSOCKET_CONNECTOR),

    AUTHENTICATION(ModelKeys.AUTHENTICATION),
    ENCRYPTION(ModelKeys.ENCRYPTION),
    SECURITY(ModelKeys.SECURITY),
    TOPOLOGY_STATE_TRANSFER(ModelKeys.TOPOLOGY_STATE_TRANSFER),

    INCLUDE_MECHANISMS(ModelKeys.MECHANISMS),
    QOP(ModelKeys.QOP),
    POLICY(ModelKeys.POLICY),
    PROPERTY(ModelKeys.PROPERTY),
    SASL(ModelKeys.SASL),
    STRENGTH(ModelKeys.STRENGTH),
    FORWARD_SECRECY(ModelKeys.FORWARD_SECRECY),
    NO_ACTIVE(ModelKeys.NO_ACTIVE),
    NO_ANONYMOUS(ModelKeys.NO_ANONYMOUS),
    NO_DICTIONARY(ModelKeys.NO_DICTIONARY),
    NO_PLAIN_TEXT(ModelKeys.NO_PLAIN_TEXT),
    PASS_CREDENTIALS(ModelKeys.PASS_CREDENTIALS),
    ;

    private final String name;
    private final AttributeDefinition definition;

    Element(final String name) {
        this.name = name;
        this.definition = null;
    }

    Element(final AttributeDefinition definition) {
        this.name = definition.getXmlName();
        this.definition = definition;
    }

    /**
     * Get the local name of this element.
     *
     * @return the local name
     */
    public String getLocalName() {
        return name;
    }

    public AttributeDefinition getDefinition() {
        return definition;
    }

    private static final Map<String, Element> elements;

    static {
        final Map<String, Element> map = new HashMap<String, Element>();
        for (Element element : values()) {
            final String name = element.getLocalName();
            if (name != null) map.put(name, element);
        }
        elements = map;
    }

    public static Element forName(String localName) {
        final Element element = elements.get(localName);
        return element == null ? UNKNOWN : element;
    }
}
