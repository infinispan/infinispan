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

import java.util.HashMap;
import java.util.Map;

/**
 * An enumeration of all the recognized XML element local names, by name.
 *
 * @author Pete Muir
 */
public enum Element {
    // must be first
    UNKNOWN(null),

    //
    ADVANCED_EXTERNALIZER("advancedExternalizer"),
    ADVANCED_EXTERNALIZERS("advancedExternalizers"),
    ASYNC("async"),
    ASYNC_LISTENER_EXECUTOR("asyncListenerExecutor"),
    ASYNC_TRANSPORT_EXECUTOR("asyncTransportExecutor"),
    REMOTE_COMMNAND_EXECUTOR("remoteCommandsExecutor"),
    CLUSTERING("clustering"),
    CLUSTER_LOADER("clusterLoader"),
    COMPATIBILITY("compatibility"),
    CUSTOM_INTERCEPTORS("customInterceptors"),
    DATA_CONTAINER("dataContainer"),
    DEADLOCK_DETECTION("deadlockDetection"),
    DEFAULT("default"),
    EVICTION("eviction"),
    EVICTION_SCHEDULED_EXECUTOR("evictionScheduledExecutor"),
    EXPIRATION("expiration"),
    FILE_STORE("fileStore"),
    GROUPS("groups"),
    GROUPER("grouper"),
    GLOBAL("global"),
    GLOBAL_JMX_STATISTICS("globalJmxStatistics"),
    HASH("hash"),
    INDEXING("indexing"),
    INTERCEPTOR("interceptor"),
    INVOCATION_BATCHING("invocationBatching"),
    JMX_STATISTICS("jmxStatistics"),
    L1("l1"),
    LAZY_DESERIALIZATION("lazyDeserialization"),
    LOADER("loader"),
    LOADERS("loaders"),
    LOCKING("locking"),
    MODULES("modules"),
    NAMED_CACHE("namedCache"),
    PROPERTIES("properties"),
    PROPERTY("property"),
    RECOVERY("recovery"),
    REPLICATION_QUEUE_SCHEDULED_EXECUTOR("replicationQueueScheduledExecutor"),
    ROOT("infinispan"),
    SERIALIZATION("serialization"),
    SHUTDOWN("shutdown"),
    SINGLETON_STORE("singletonStore"),
    STATE_RETRIEVAL("stateRetrieval"),
    STATE_TRANSFER("stateTransfer"),
    STORE("store"),
    STORE_AS_BINARY("storeAsBinary"),
    SYNC("sync"),
    TRANSACTION("transaction"),
    TRANSPORT("transport"),
    UNSAFE("unsafe"),
    VERSIONING("versioning"),
    SITES("sites"),
    SITE("site"),
    BACKUPS("backups"),
    BACKUP("backup"),
    BACKUP_FOR("backupFor"),
    TAKE_OFFLINE("takeOffline"),
    TOTAL_ORDER_EXECUTOR("totalOrderExecutor"),
    ;

    private final String name;

    Element(final String name) {
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

    private static final Map<String, Element> MAP;

    static {
        final Map<String, Element> map = new HashMap<String, Element>(8);
        for (Element element : values()) {
            final String name = element.getLocalName();
            if (name != null) map.put(name, element);
        }
        MAP = map;
    }

    public static Element forName(String localName) {
        final Element element = MAP.get(localName);
        return element == null ? UNKNOWN : element;
    }
}
