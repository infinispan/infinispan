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

/**
 * Enumerates the attributes used in the Infinispan Endpoint subsystem schema.
 *
 * @author Tristan Tarrant
 * @since 5.3
 */
public enum Attribute {
    // must be first
    UNKNOWN(null),

    AWAIT_INITIAL_RETRIEVAL(ModelKeys.AWAIT_INITIAL_RETRIEVAL),
    ALLOW_CREDENTIALS(ModelKeys.ALLOW_CREDENTIALS),
    AUTH_METHOD(ModelKeys.AUTH_METHOD),
    CONTEXT_PATH(ModelKeys.CONTEXT_PATH),
    CACHE(ModelKeys.CACHE),
    CACHE_CONTAINER(ModelKeys.CACHE_CONTAINER),
    @Deprecated
    CACHE_SUFFIX(ModelKeys.CACHE_SUFFIX),
    CLIENT_ENCODING(ModelKeys.CLIENT_ENCODING),
    IGNORED_CACHES(ModelKeys.IGNORED_CACHES),
    IO_THREADS(ModelKeys.IO_THREADS),
    EXTENDED_HEADERS(ModelKeys.EXTENDED_HEADERS),
    EXTERNAL_HOST(ModelKeys.EXTERNAL_HOST),
    EXTERNAL_PORT(ModelKeys.EXTERNAL_PORT),
    IDLE_TIMEOUT(ModelKeys.IDLE_TIMEOUT),
    MECHANISMS(ModelKeys.MECHANISMS),
    HOST_NAME(ModelKeys.HOST_NAME),
    PATH(ModelKeys.PATH),
    LAZY_RETRIEVAL(ModelKeys.LAZY_RETRIEVAL),
    LOCK_TIMEOUT(ModelKeys.LOCK_TIMEOUT),
    REPLICATION_TIMEOUT(ModelKeys.REPLICATION_TIMEOUT),
    MAX_CONTENT_LENGTH(ModelKeys.MAX_CONTENT_LENGTH),
    MAX_AGE_SECONDS(ModelKeys.MAX_AGE_SECONDS),
    COMPRESSION_LEVEL(ModelKeys.COMPRESSION_LEVEL),
    NAME(ModelKeys.NAME),
    QOP(ModelKeys.QOP),
    RECEIVE_BUFFER_SIZE(ModelKeys.RECEIVE_BUFFER_SIZE),
    REQUIRE_SSL_CLIENT_AUTH(ModelKeys.REQUIRE_SSL_CLIENT_AUTH),
    SEND_BUFFER_SIZE(ModelKeys.SEND_BUFFER_SIZE),
    SECURITY_DOMAIN(ModelKeys.SECURITY_DOMAIN),
    SECURITY_MODE(ModelKeys.SECURITY_MODE),
    SECURITY_REALM(ModelKeys.SECURITY_REALM),
    SERVER_CONTEXT_NAME(ModelKeys.SERVER_CONTEXT_NAME),
    SERVER_NAME(ModelKeys.SERVER_NAME),
    SOCKET_BINDING(ModelKeys.SOCKET_BINDING),
    REST_SOCKET_BINDING(ModelKeys.REST_SOCKET_BINDING),
    HOTROD_SOCKET_BINDING(ModelKeys.HOTROD_SOCKET_BINDING),
    SINGLE_PORT_SOCKET_BINDING(ModelKeys.SINGLE_PORT_SOCKET_BINDING),
    SSL(ModelKeys.SSL),
    STRENGTH(ModelKeys.STRENGTH),
    TCP_NODELAY(ModelKeys.TCP_NODELAY),
    TCP_KEEPALIVE(ModelKeys.TCP_KEEPALIVE),
    KEEP_ALIVE(ModelKeys.KEEP_ALIVE),
    UPDATE_TIMEOUT(ModelKeys.UPDATE_TIMEOUT),
    VALUE(ModelKeys.VALUE),
    VIRTUAL_HOST(ModelKeys.VIRTUAL_HOST),
    VIRTUAL_SERVER(ModelKeys.VIRTUAL_SERVER),
    WORKER_THREADS(ModelKeys.WORKER_THREADS);

    private final String name;

    Attribute(String name) {
        this.name = name;
    }

    /**
     * Get the local name of this element.
     *
     * @return the local name
     */
    public String getLocalName() {
        return name;
    }

    private static final Map<String, Attribute> attributes;

    static {
        final Map<String, Attribute> map = new HashMap<>();
        for (Attribute attribute : values()) {
            final String name = attribute.getLocalName();
            if (name != null) map.put(name, attribute);
        }
        attributes = map;
    }

    public static Attribute forName(String localName) {
        final Attribute attribute = attributes.get(localName);
        return attribute == null ? UNKNOWN : attribute;
    }
}
