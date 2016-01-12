/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2012, Red Hat, Inc., and individual contributors
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.stream.XMLStreamException;

import org.infinispan.security.impl.ClusterRoleMapper;
import org.infinispan.security.impl.CommonNameRoleMapper;
import org.infinispan.security.impl.IdentityRoleMapper;
import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.persistence.SubsystemMarshallingContext;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;
import org.jboss.dmr.Property;
import org.jboss.staxmapper.XMLElementWriter;
import org.jboss.staxmapper.XMLExtendedStreamWriter;

/**
 * XML writer for current Infinispan subsystem schema version.
 * @author Paul Ferraro
 * @author Richard Achmatowicz (c) 2011 Red Hat Inc.
 * @author Tristan Tarrant
 */
public class InfinispanSubsystemXMLWriter implements XMLElementWriter<SubsystemMarshallingContext> {
    public static final XMLElementWriter<SubsystemMarshallingContext> INSTANCE = new InfinispanSubsystemXMLWriter();

    /**
     * {@inheritDoc}
     * @see org.jboss.staxmapper.XMLElementWriter#writeContent(org.jboss.staxmapper.XMLExtendedStreamWriter, java.lang.Object)
     */
    @Override
    public void writeContent(XMLExtendedStreamWriter writer, SubsystemMarshallingContext context) throws XMLStreamException {
        context.startSubsystemElement(Namespace.CURRENT.getUri(), false);
        ModelNode model = context.getModelNode();
        if (model.isDefined()) {
            for (Property entry: model.get(ModelKeys.CACHE_CONTAINER).asPropertyList()) {

                String containerName = entry.getName();
                ModelNode container = entry.getValue();

                writer.writeStartElement(Element.CACHE_CONTAINER.getLocalName());
                writer.writeAttribute(Attribute.NAME.getLocalName(), containerName);
                // AS7-3488 make default-cache a non required attribute
                // this.writeRequired(writer, Attribute.DEFAULT_CACHE, container, ModelKeys.DEFAULT_CACHE);
                this.writeListAsAttribute(writer, Attribute.ALIASES, container, ModelKeys.ALIASES);
                this.writeOptional(writer, Attribute.DEFAULT_CACHE, container, ModelKeys.DEFAULT_CACHE);
                this.writeOptional(writer, Attribute.JNDI_NAME, container, ModelKeys.JNDI_NAME);
                this.writeOptional(writer, Attribute.START, container, ModelKeys.START);
                this.writeOptional(writer, Attribute.MODULE, container, ModelKeys.MODULE);
                this.writeOptional(writer, Attribute.STATISTICS, container, ModelKeys.STATISTICS);

                if (container.hasDefined(ModelKeys.TRANSPORT)) {
                    writer.writeStartElement(Element.TRANSPORT.getLocalName());
                    ModelNode transport = container.get(ModelKeys.TRANSPORT, ModelKeys.TRANSPORT_NAME);
                    this.writeOptional(writer, Attribute.CHANNEL, transport, ModelKeys.CHANNEL);
                    this.writeOptional(writer, Attribute.LOCK_TIMEOUT, transport, ModelKeys.LOCK_TIMEOUT);
                    this.writeOptional(writer, Attribute.STRICT_PEER_TO_PEER, transport, ModelKeys.STRICT_PEER_TO_PEER);
                    writer.writeEndElement();
                }

                if (container.hasDefined(ModelKeys.SECURITY)) {
                    writer.writeStartElement(Element.SECURITY.getLocalName());
                    ModelNode security = container.get(ModelKeys.SECURITY, ModelKeys.SECURITY_NAME);
                    if (security.hasDefined(ModelKeys.AUTHORIZATION)) {
                        writer.writeStartElement(Element.AUTHORIZATION.getLocalName());
                        ModelNode authorization = security.get(ModelKeys.AUTHORIZATION, ModelKeys.AUTHORIZATION_NAME);
                        if (authorization.hasDefined(ModelKeys.MAPPER)) {
                            String mapper = authorization.get(ModelKeys.MAPPER).asString();
                            if (CommonNameRoleMapper.class.getName().equals(mapper)) {
                                writer.writeEmptyElement(Element.COMMON_NAME_ROLE_MAPPER.getLocalName());
                            } else if (ClusterRoleMapper.class.getName().equals(mapper)) {
                                writer.writeEmptyElement(Element.CLUSTER_ROLE_MAPPER.getLocalName());
                            } else if (IdentityRoleMapper.class.getName().equals(mapper)) {
                                writer.writeEmptyElement(Element.IDENTITY_ROLE_MAPPER.getLocalName());
                            } else {
                                writer.writeStartElement(Element.CUSTOM_ROLE_MAPPER.getLocalName());
                                writer.writeAttribute(Attribute.CLASS.getLocalName(), mapper);
                                writer.writeEndElement();
                            }
                        }

                        ModelNode roles = authorization.get(ModelKeys.ROLE);
                        for(ModelNode roleNode : roles.asList()) {
                            ModelNode role = roleNode.get(0);
                            writer.writeStartElement(Element.ROLE.getLocalName());
                            AuthorizationRoleResource.NAME.marshallAsAttribute(role, writer);
                            this.writeListAsAttribute(writer, Attribute.PERMISSIONS, role, ModelKeys.PERMISSIONS);
                            writer.writeEndElement();
                        }

                        writer.writeEndElement();
                    }

                    writer.writeEndElement();
                }

                if (container.hasDefined(ModelKeys.GLOBAL_STATE)) {
                    writer.writeStartElement(Element.GLOBAL_STATE.getLocalName());
                    ModelNode globalState = container.get(ModelKeys.GLOBAL_STATE, ModelKeys.GLOBAL_STATE_NAME);
                    writeStatePathElement(Element.PERSISTENT_LOCATION, ModelKeys.PERSISTENT_LOCATION, writer, globalState);
                    writeStatePathElement(Element.TEMPORARY_LOCATION, ModelKeys.TEMPORARY_LOCATION, writer, globalState);
                    writer.writeEndElement();
                }

                // write any configured thread pools
                if (container.hasDefined(ThreadPoolResource.WILDCARD_PATH.getKey())) {
                    writeThreadPoolElements(Element.ASYNC_OPERATIONS_THREAD_POOL, ThreadPoolResource.ASYNC_OPERATIONS, writer, container);
                    writeScheduledThreadPoolElements(Element.EXPIRATION_THREAD_POOL, ScheduledThreadPoolResource.EXPIRATION, writer, container);
                    writeThreadPoolElements(Element.LISTENER_THREAD_POOL, ThreadPoolResource.LISTENER, writer, container);
                    writeThreadPoolElements(Element.PERSISTENCE_THREAD_POOL, ThreadPoolResource.PERSISTENCE, writer, container);
                    writeThreadPoolElements(Element.REMOTE_COMMAND_THREAD_POOL, ThreadPoolResource.REMOTE_COMMAND, writer, container);
                    writeScheduledThreadPoolElements(Element.REPLICATION_QUEUE_THREAD_POOL, ScheduledThreadPoolResource.REPLICATION_QUEUE, writer, container);
                    writeThreadPoolElements(Element.STATE_TRANSFER_THREAD_POOL, ThreadPoolResource.STATE_TRANSFER, writer, container);
                    writeThreadPoolElements(Element.TRANSPORT_THREAD_POOL, ThreadPoolResource.TRANSPORT, writer, container);
                }

                ModelNode configurations = container.get(ModelKeys.CONFIGURATIONS, ModelKeys.CONFIGURATIONS_NAME);

                // write any existent cache types
                processCacheConfiguration(writer, container, configurations, ModelKeys.LOCAL_CACHE);
                processCacheConfiguration(writer, container, configurations, ModelKeys.INVALIDATION_CACHE);
                processCacheConfiguration(writer, container, configurations, ModelKeys.REPLICATED_CACHE);
                processCacheConfiguration(writer, container, configurations, ModelKeys.DISTRIBUTED_CACHE);

                writer.writeEndElement();
            }
        }
        writer.writeEndElement();
    }

    private void writeStatePathElement(Element element, String name, XMLExtendedStreamWriter writer, ModelNode node) throws XMLStreamException {
        if (node.hasDefined(name)) {
            ModelNode pathNode = node.get(name);
            writer.writeStartElement(element.getLocalName());
            writeAttribute(writer, pathNode, GlobalStateResource.PATH);
            writeOptional(writer, Attribute.RELATIVE_TO, pathNode, ModelKeys.RELATIVE_TO);
            writer.writeEndElement();
        }

    }

    private static void writeThreadPoolElements(Element element, ThreadPoolResource pool, XMLExtendedStreamWriter writer, ModelNode container) throws XMLStreamException {
        if (container.get(pool.getPathElement().getKey()).hasDefined(pool.getPathElement().getValue())) {
            ModelNode threadPool = container.get(pool.getPathElement().getKeyValuePair());
            if (hasDefined(threadPool, pool.getAttributes())) {
                writer.writeStartElement(element.getLocalName());
                writeAttributes(writer, threadPool, pool.getAttributes());
                writer.writeEndElement();
            }
        }
    }

    private static void writeScheduledThreadPoolElements(Element element, ScheduledThreadPoolResource pool, XMLExtendedStreamWriter writer, ModelNode container) throws XMLStreamException {
        if (container.get(pool.getPathElement().getKey()).hasDefined(pool.getPathElement().getValue())) {
            ModelNode threadPool = container.get(pool.getPathElement().getKeyValuePair());
            if (hasDefined(threadPool, pool.getAttributes())) {
                writer.writeStartElement(element.getLocalName());
                writeAttributes(writer, threadPool, pool.getAttributes());
                writer.writeEndElement();
            }
        }
    }

    private void processCacheConfiguration(XMLExtendedStreamWriter writer, ModelNode container, ModelNode configurations, String cacheType)
            throws XMLStreamException {
        String cacheConfigurationType = cacheType + ModelKeys.CONFIGURATION_SUFFIX;
        Map<String, List<String>> configurationMappings = new HashMap<>();
        if (container.get(cacheType).isDefined()) {
            for (Property cacheEntry : container.get(cacheType).asPropertyList()) {
                String cacheName = cacheEntry.getName();
                String configurationName = cacheEntry.getValue().get(ModelKeys.CONFIGURATION).asString();

                configurationMappings.compute(configurationName, (k, v) -> {
                    if (v == null) {
                        v = new ArrayList<>();
                    }
                    v.add(cacheName);
                    return v;
                });
            }
        }
        if (configurations.get(cacheConfigurationType).isDefined()) {
            for (Property cacheEntry : configurations.get(cacheConfigurationType).asPropertyList()) {
                String name = cacheEntry.getName();
                ModelNode cacheConfiguration = cacheEntry.getValue();

                Element element;
                boolean identity = false;
                List<String> caches = configurationMappings.get(name);
                if (caches != null && caches.size() == 1 && caches.get(0).equals(name)) {
                    element = Element.forName(cacheType);
                    identity = true;
                } else {
                    element = Element.forName(cacheConfigurationType);
                }

                writer.writeStartElement(element.getLocalName());
                // write identifier before other attributes
                writer.writeAttribute(Attribute.NAME.getLocalName(), name);

                switch(cacheType) {
                    case ModelKeys.DISTRIBUTED_CACHE:
                        processDistributedCacheAttributes(writer, cacheConfiguration);
                    case ModelKeys.REPLICATED_CACHE:
                    case ModelKeys.INVALIDATION_CACHE:
                        processCommonClusteredCacheAttributes(writer, cacheConfiguration);
                    default:
                        processCommonCacheConfigurationAttributesElements(writer, cacheConfiguration);
                }

                writer.writeEndElement();

                // Now the concrete instances
                if (!identity && caches!= null) {
                    for (String cache : caches) {
                        writer.writeStartElement(Element.forName(cacheType).getLocalName());
                        writer.writeAttribute(Attribute.NAME.getLocalName(), cache);
                        writer.writeAttribute(Attribute.CONFIGURATION.getLocalName(), name);
                        writer.writeEndElement();
                    }
                }
            }
        }
    }

    private void processDistributedCacheAttributes(XMLExtendedStreamWriter writer, ModelNode distributedCache) throws XMLStreamException {
        this.writeOptional(writer, Attribute.OWNERS, distributedCache, ModelKeys.OWNERS);
        this.writeOptional(writer, Attribute.SEGMENTS, distributedCache, ModelKeys.SEGMENTS);
        this.writeOptional(writer, Attribute.CAPACITY_FACTOR, distributedCache, ModelKeys.CAPACITY_FACTOR);
        this.writeOptional(writer, Attribute.L1_LIFESPAN, distributedCache, ModelKeys.L1_LIFESPAN);
    }

    private void processCommonClusteredCacheAttributes(XMLExtendedStreamWriter writer, ModelNode cache)
            throws XMLStreamException {
        this.writeOptional(writer, Attribute.MODE, cache, ModelKeys.MODE);
        this.writeOptional(writer, Attribute.QUEUE_SIZE, cache, ModelKeys.QUEUE_SIZE);
        this.writeOptional(writer, Attribute.QUEUE_FLUSH_INTERVAL, cache, ModelKeys.QUEUE_FLUSH_INTERVAL);
        this.writeOptional(writer, Attribute.REMOTE_TIMEOUT, cache, ModelKeys.REMOTE_TIMEOUT);
    }

    private void processCommonCacheConfigurationAttributesElements(XMLExtendedStreamWriter writer, ModelNode cache)
            throws XMLStreamException {

        this.writeOptional(writer, Attribute.CONFIGURATION, cache, ModelKeys.CONFIGURATION);
        this.writeOptional(writer, Attribute.START, cache, ModelKeys.START);
        this.writeOptional(writer, Attribute.BATCHING, cache, ModelKeys.BATCHING);
        this.writeOptional(writer, Attribute.JNDI_NAME, cache, ModelKeys.JNDI_NAME);
        this.writeOptional(writer, Attribute.MODULE, cache, ModelKeys.MODULE);
        this.writeOptional(writer, Attribute.SIMPLE_CACHE, cache, ModelKeys.SIMPLE_CACHE);
        this.writeOptional(writer, Attribute.STATISTICS, cache, ModelKeys.STATISTICS);
        this.writeOptional(writer, Attribute.STATISTICS_AVAILABLE, cache, ModelKeys.STATISTICS_AVAILABLE);
        this.writeOptional(writer, Attribute.INLINE_INTERCEPTORS, cache, ModelKeys.INLINE_INTERCEPTORS);

        if (cache.get(ModelKeys.BACKUP).isDefined()) {
            writer.writeStartElement(Element.BACKUPS.getLocalName());
            for (Property property : cache.get(ModelKeys.BACKUP).asPropertyList()) {
                writer.writeStartElement(Element.BACKUP.getLocalName());
                writer.writeAttribute(Attribute.SITE.getLocalName(), property.getName());
                ModelNode backup = property.getValue();
                BackupSiteConfigurationResource.FAILURE_POLICY.marshallAsAttribute(backup, writer);
                BackupSiteConfigurationResource.STRATEGY.marshallAsAttribute(backup, writer);
                BackupSiteConfigurationResource.REPLICATION_TIMEOUT.marshallAsAttribute(backup, writer);
                BackupSiteConfigurationResource.ENABLED.marshallAsAttribute(backup, writer);
                if (backup.hasDefined(ModelKeys.TAKE_BACKUP_OFFLINE_AFTER_FAILURES)
                        || backup.hasDefined(ModelKeys.TAKE_BACKUP_OFFLINE_MIN_WAIT)) {
                    writer.writeStartElement(Element.TAKE_OFFLINE.getLocalName());
                    BackupSiteConfigurationResource.TAKE_OFFLINE_AFTER_FAILURES.marshallAsAttribute(backup, writer);
                    BackupSiteConfigurationResource.TAKE_OFFLINE_MIN_WAIT.marshallAsAttribute(backup, writer);
                    writer.writeEndElement();
                }
                if (backup.get(ModelKeys.STATE_TRANSFER, ModelKeys.STATE_TRANSFER_NAME).isDefined()) {
                    ModelNode stateTransfer = backup.get(ModelKeys.STATE_TRANSFER, ModelKeys.STATE_TRANSFER_NAME);
                    if (stateTransfer.hasDefined(ModelKeys.CHUNK_SIZE)
                          || stateTransfer.hasDefined(ModelKeys.TIMEOUT)
                          || stateTransfer.hasDefined(ModelKeys.MAX_RETRIES)
                          || stateTransfer.hasDefined(ModelKeys.WAIT_TIME)) {
                       writer.writeStartElement(Element.STATE_TRANSFER.getLocalName());
                       BackupSiteStateTransferConfigurationResource.STATE_TRANSFER_CHUNK_SIZE.marshallAsAttribute(stateTransfer, writer);
                       BackupSiteStateTransferConfigurationResource.STATE_TRANSFER_TIMEOUT.marshallAsAttribute(stateTransfer, writer);
                       BackupSiteStateTransferConfigurationResource.STATE_TRANSFER_MAX_RETRIES.marshallAsAttribute(stateTransfer, writer);
                       BackupSiteStateTransferConfigurationResource.STATE_TRANSFER_WAIT_TIME.marshallAsAttribute(stateTransfer, writer);
                       writer.writeEndElement();
                    }
                }
                writer.writeEndElement();
            }
            writer.writeEndElement();
        }

        if (cache.get(ModelKeys.REMOTE_CACHE).isDefined() || cache.get(ModelKeys.REMOTE_SITE).isDefined()) {
            writer.writeStartElement(Element.BACKUP_FOR.getLocalName());
            CacheConfigurationResource.REMOTE_CACHE.marshallAsAttribute(cache, writer);
            CacheConfigurationResource.REMOTE_SITE.marshallAsAttribute(cache, writer);
            writer.writeEndElement();
        }

        if (cache.get(ModelKeys.LOCKING, ModelKeys.LOCKING_NAME).isDefined()) {
            writer.writeStartElement(Element.LOCKING.getLocalName());
            ModelNode locking = cache.get(ModelKeys.LOCKING, ModelKeys.LOCKING_NAME);
            this.writeOptional(writer, Attribute.ISOLATION, locking, ModelKeys.ISOLATION);
            this.writeOptional(writer, Attribute.STRIPING, locking, ModelKeys.STRIPING);
            this.writeOptional(writer, Attribute.ACQUIRE_TIMEOUT, locking, ModelKeys.ACQUIRE_TIMEOUT);
            this.writeOptional(writer, Attribute.CONCURRENCY_LEVEL, locking, ModelKeys.CONCURRENCY_LEVEL);
            writer.writeEndElement();
        }

        if (cache.get(ModelKeys.TRANSACTION, ModelKeys.TRANSACTION_NAME).isDefined()) {
            writer.writeStartElement(Element.TRANSACTION.getLocalName());
            ModelNode transaction = cache.get(ModelKeys.TRANSACTION, ModelKeys.TRANSACTION_NAME);
            this.writeOptional(writer, Attribute.STOP_TIMEOUT, transaction, ModelKeys.STOP_TIMEOUT);
            this.writeOptional(writer, Attribute.MODE, transaction, ModelKeys.MODE);
            this.writeOptional(writer, Attribute.LOCKING, transaction, ModelKeys.LOCKING);
            this.writeOptional(writer, Attribute.NOTIFICATIONS, transaction, ModelKeys.NOTIFICATIONS);
            writer.writeEndElement();
        }

        if (cache.get(ModelKeys.EVICTION, ModelKeys.EVICTION_NAME).isDefined()) {
            writer.writeStartElement(Element.EVICTION.getLocalName());
            ModelNode eviction = cache.get(ModelKeys.EVICTION, ModelKeys.EVICTION_NAME);
            this.writeOptional(writer, Attribute.STRATEGY, eviction, ModelKeys.STRATEGY);
            this.writeOptional(writer, Attribute.MAX_ENTRIES, eviction, ModelKeys.MAX_ENTRIES);
            this.writeOptional(writer, Attribute.TYPE, eviction, ModelKeys.TYPE);
            this.writeOptional(writer, Attribute.SIZE, eviction, ModelKeys.SIZE);
            writer.writeEndElement();
        }

        if (cache.get(ModelKeys.EXPIRATION, ModelKeys.EXPIRATION_NAME).isDefined()) {
            writer.writeStartElement(Element.EXPIRATION.getLocalName());
            ModelNode expiration = cache.get(ModelKeys.EXPIRATION, ModelKeys.EXPIRATION_NAME);
            this.writeOptional(writer, Attribute.MAX_IDLE, expiration, ModelKeys.MAX_IDLE);
            this.writeOptional(writer, Attribute.LIFESPAN, expiration, ModelKeys.LIFESPAN);
            this.writeOptional(writer, Attribute.INTERVAL, expiration, ModelKeys.INTERVAL);
            writer.writeEndElement();
        }

        if (cache.get(ModelKeys.COMPATIBILITY).isDefined()) {
            ModelNode compatibility = cache.get(ModelKeys.COMPATIBILITY, ModelKeys.COMPATIBILITY_NAME);
            writer.writeStartElement(Element.COMPATIBILITY.getLocalName());
            CompatibilityConfigurationResource.ENABLED.marshallAsAttribute(compatibility, writer);
            CompatibilityConfigurationResource.MARSHALLER.marshallAsAttribute(compatibility, writer);
            writer.writeEndElement();
        }

        if (cache.hasDefined(ModelKeys.SECURITY)) {
            writer.writeStartElement(Element.SECURITY.getLocalName());
            ModelNode security = cache.get(ModelKeys.SECURITY, ModelKeys.SECURITY_NAME);
            if (security.hasDefined(ModelKeys.AUTHORIZATION)) {
                writer.writeStartElement(Element.AUTHORIZATION.getLocalName());
                ModelNode authorization = security.get(ModelKeys.AUTHORIZATION, ModelKeys.AUTHORIZATION_NAME);
                CacheAuthorizationConfigurationResource.ENABLED.marshallAsAttribute(authorization, writer);
                this.writeListAsAttribute(writer, Attribute.ROLES, authorization, ModelKeys.ROLES);
                writer.writeEndElement();
            }

            writer.writeEndElement();
        }

        if (cache.get(ModelKeys.LOADER).isDefined()) {
            for (Property clusterLoaderEntry : cache.get(ModelKeys.LOADER).asPropertyList()) {
                ModelNode loader = clusterLoaderEntry.getValue();
                writer.writeStartElement(Element.LOADER.getLocalName());
                // write identifier before other attributes
                ModelNode name = new ModelNode();
                name.get(ModelKeys.NAME).set(clusterLoaderEntry.getName());
                LoaderConfigurationResource.NAME.marshallAsAttribute(name, false, writer);
                this.writeRequired(writer, Attribute.CLASS, loader, ModelKeys.CLASS);
                this.writeLoaderAttributes(writer, loader);
                this.writeStoreProperties(writer, loader);
                writer.writeEndElement();
            }
        }

        if (cache.get(ModelKeys.CLUSTER_LOADER).isDefined()) {
            for (Property clusterLoaderEntry : cache.get(ModelKeys.CLUSTER_LOADER).asPropertyList()) {
                ModelNode loader = clusterLoaderEntry.getValue();
                writer.writeStartElement(Element.CLUSTER_LOADER.getLocalName());
                // write identifier before other attributes
                ModelNode name = new ModelNode();
                name.get(ModelKeys.NAME).set(clusterLoaderEntry.getName());
                ClusterLoaderConfigurationResource.NAME.marshallAsAttribute(name, false, writer);
                this.writeOptional(writer, Attribute.REMOTE_TIMEOUT, loader, ModelKeys.REMOTE_TIMEOUT);
                this.writeLoaderAttributes(writer, loader);
                this.writeStoreProperties(writer, loader);
                writer.writeEndElement();
            }
        }

        if (cache.get(ModelKeys.STORE).isDefined()) {
            for (Property storeEntry : cache.get(ModelKeys.STORE).asPropertyList()) {
                ModelNode store = storeEntry.getValue();
                writer.writeStartElement(Element.STORE.getLocalName());
                // write identifier before other attributes
                ModelNode name = new ModelNode();
                name.get(ModelKeys.NAME).set(storeEntry.getName());
                StoreConfigurationResource.NAME.marshallAsAttribute(name, false, writer);
                this.writeRequired(writer, Attribute.CLASS, store, ModelKeys.CLASS);
                this.writeStoreAttributes(writer, store);
                this.writeStoreWriteBehind(writer, store);
                this.writeStoreProperties(writer, store);
                writer.writeEndElement();
            }
        }

        if (cache.get(ModelKeys.FILE_STORE).isDefined()) {
            for (Property fileStoreEntry : cache.get(ModelKeys.FILE_STORE).asPropertyList()) {
                ModelNode store = fileStoreEntry.getValue();
                writer.writeStartElement(Element.FILE_STORE.getLocalName());
                // write identifier before other attributes
                ModelNode name = new ModelNode();
                name.get(ModelKeys.NAME).set(fileStoreEntry.getName());
                FileStoreResource.NAME.marshallAsAttribute(name, false, writer);
                this.writeOptional(writer, Attribute.MAX_ENTRIES, store, ModelKeys.MAX_ENTRIES);
                this.writeOptional(writer, Attribute.RELATIVE_TO, store, ModelKeys.RELATIVE_TO);
                this.writeOptional(writer, Attribute.PATH, store, ModelKeys.PATH);
                this.writeStoreAttributes(writer, store);
                this.writeStoreWriteBehind(writer, store);
                this.writeStoreProperties(writer, store);
                writer.writeEndElement();
            }
        }

        if (cache.get(ModelKeys.STRING_KEYED_JDBC_STORE).isDefined()) {
            for (Property stringKeyedJDBCStoreEntry : cache.get(ModelKeys.STRING_KEYED_JDBC_STORE).asPropertyList()) {
                ModelNode store = stringKeyedJDBCStoreEntry.getValue();
                writer.writeStartElement(Element.STRING_KEYED_JDBC_STORE.getLocalName());
                // write identifier before other attributes
                ModelNode name = new ModelNode();
                name.get(ModelKeys.NAME).set(stringKeyedJDBCStoreEntry.getName());
                StringKeyedJDBCStoreResource.NAME.marshallAsAttribute(name, false, writer);
                this.writeJdbcStoreAttributes(writer, store);
                this.writeStoreWriteBehind(writer, store);
                this.writeStoreProperties(writer, store);
                this.writeJDBCStoreTable(writer, Element.STRING_KEYED_TABLE, store, ModelKeys.STRING_KEYED_TABLE);
                writer.writeEndElement();
            }
        }

        if (cache.get(ModelKeys.BINARY_KEYED_JDBC_STORE).isDefined()) {
            for (Property binaryKeyedJDBCStoreEntry : cache.get(ModelKeys.BINARY_KEYED_JDBC_STORE).asPropertyList()) {
                ModelNode store = binaryKeyedJDBCStoreEntry.getValue();
                writer.writeStartElement(Element.BINARY_KEYED_JDBC_STORE.getLocalName());
                // write identifier before other attributes
                ModelNode name = new ModelNode();
                name.get(ModelKeys.NAME).set(binaryKeyedJDBCStoreEntry.getName());
                BinaryKeyedJDBCStoreConfigurationResource.NAME.marshallAsAttribute(name, false, writer);
                this.writeJdbcStoreAttributes(writer, store);
                this.writeStoreWriteBehind(writer, store);
                this.writeStoreProperties(writer, store);
                this.writeJDBCStoreTable(writer, Element.BINARY_KEYED_TABLE, store, ModelKeys.BINARY_KEYED_TABLE);
                writer.writeEndElement();
            }
        }

        if (cache.get(ModelKeys.MIXED_KEYED_JDBC_STORE).isDefined()) {
            for (Property mixedKeyedJDBCStoreEntry : cache.get(ModelKeys.MIXED_KEYED_JDBC_STORE).asPropertyList()) {
                ModelNode store = mixedKeyedJDBCStoreEntry.getValue();
                writer.writeStartElement(Element.MIXED_KEYED_JDBC_STORE.getLocalName());
                // write identifier before other attributes
                ModelNode name = new ModelNode();
                name.get(ModelKeys.NAME).set(mixedKeyedJDBCStoreEntry.getName());
                MixedKeyedJDBCStoreConfigurationResource.NAME.marshallAsAttribute(name, false, writer);
                this.writeJdbcStoreAttributes(writer, store);
                this.writeStoreWriteBehind(writer, store);
                this.writeStoreProperties(writer, store);
                this.writeJDBCStoreTable(writer, Element.BINARY_KEYED_TABLE, store, ModelKeys.BINARY_KEYED_TABLE);
                this.writeJDBCStoreTable(writer, Element.STRING_KEYED_TABLE, store, ModelKeys.STRING_KEYED_TABLE);
                writer.writeEndElement();
            }
        }

        if (cache.get(ModelKeys.LEVELDB_STORE).isDefined()) {
            for (Property levelDbStoreEntry : cache.get(ModelKeys.LEVELDB_STORE).asPropertyList()) {
                ModelNode store = levelDbStoreEntry.getValue();
                writer.writeStartElement(Element.LEVELDB_STORE.getLocalName());
                // write identifier before other attributes
                ModelNode name = new ModelNode();
                name.get(ModelKeys.NAME).set(levelDbStoreEntry.getName());
                LevelDBStoreConfigurationResource.NAME.marshallAsAttribute(name, false, writer);
                this.writeOptional(writer, Attribute.RELATIVE_TO, store, ModelKeys.RELATIVE_TO);
                this.writeOptional(writer, Attribute.PATH, store, ModelKeys.PATH);
                this.writeOptional(writer, Attribute.BLOCK_SIZE, store, ModelKeys.BLOCK_SIZE);
                this.writeOptional(writer, Attribute.CACHE_SIZE, store, ModelKeys.CACHE_SIZE);
                this.writeOptional(writer, Attribute.CLEAR_THRESHOLD, store, ModelKeys.CLEAR_THRESHOLD);
                this.writeStoreAttributes(writer, store);
                this.writeStoreExpiration(writer, store);
                this.writeStoreImplementation(writer, store);
                this.writeStoreCompression(writer, store);
                this.writeStoreProperties(writer, store);
                writer.writeEndElement();
            }
        }

        if (cache.get(ModelKeys.REMOTE_STORE).isDefined()) {
            for (Property remoteStoreEntry : cache.get(ModelKeys.REMOTE_STORE).asPropertyList()) {
                ModelNode store = remoteStoreEntry.getValue();
                writer.writeStartElement(Element.REMOTE_STORE.getLocalName());
                // write identifier before other attributes
                ModelNode name = new ModelNode();
                name.get(ModelKeys.NAME).set(remoteStoreEntry.getName());
                RemoteStoreConfigurationResource.NAME.marshallAsAttribute(name, false, writer);
                this.writeOptional(writer, Attribute.CACHE, store, ModelKeys.CACHE);
                this.writeOptional(writer, Attribute.HOTROD_WRAPPING, store, ModelKeys.HOTROD_WRAPPING);
                this.writeOptional(writer, Attribute.RAW_VALUES, store, ModelKeys.RAW_VALUES);
                this.writeOptional(writer, Attribute.SOCKET_TIMEOUT, store, ModelKeys.SOCKET_TIMEOUT);
                this.writeOptional(writer, Attribute.TCP_NO_DELAY, store, ModelKeys.TCP_NO_DELAY);
                this.writeStoreAttributes(writer, store);
                this.writeStoreWriteBehind(writer, store);
                this.writeStoreProperties(writer, store);
                for (ModelNode remoteServer: store.require(ModelKeys.REMOTE_SERVERS).asList()) {
                    writer.writeStartElement(Element.REMOTE_SERVER.getLocalName());
                    writer.writeAttribute(Attribute.OUTBOUND_SOCKET_BINDING.getLocalName(), remoteServer.get(ModelKeys.OUTBOUND_SOCKET_BINDING).asString());
                    writer.writeEndElement();
                }
                writer.writeEndElement();
            }
        }

        if (cache.get(ModelKeys.REST_STORE).isDefined()) {
            for (Property restStoreEntry : cache.get(ModelKeys.REST_STORE).asPropertyList()) {
                ModelNode store = restStoreEntry.getValue();
                writer.writeStartElement(Element.REST_STORE.getLocalName());
                // write identifier before other attributes
                ModelNode name = new ModelNode();
                name.get(ModelKeys.NAME).set(restStoreEntry.getName());
                RestStoreConfigurationResource.NAME.marshallAsAttribute(name, false, writer);
                this.writeOptional(writer, Attribute.APPEND_CACHE_NAME_TO_PATH, store, ModelKeys.APPEND_CACHE_NAME_TO_PATH);
                this.writeOptional(writer, Attribute.PATH, store, ModelKeys.PATH);

                this.writeStoreAttributes(writer, store);
                this.writeStoreWriteBehind(writer, store);
                this.writeStoreProperties(writer, store);

                for (ModelNode remoteServer: store.require(ModelKeys.REMOTE_SERVERS).asList()) {
                    writer.writeStartElement(Element.REMOTE_SERVER.getLocalName());
                    writer.writeAttribute(Attribute.OUTBOUND_SOCKET_BINDING.getLocalName(), remoteServer.get(ModelKeys.OUTBOUND_SOCKET_BINDING).asString());
                    writer.writeEndElement();
                }

                if (store.hasDefined(ModelKeys.CONNECTION_POOL)) {
                    ModelNode pool = store.get(ModelKeys.CONNECTION_POOL);
                    writer.writeStartElement(Element.CONNECTION_POOL.getLocalName());
                    this.writeOptional(writer, Attribute.CONNECTION_TIMEOUT, pool, ModelKeys.CONNECTION_TIMEOUT);
                    this.writeOptional(writer, Attribute.MAX_CONNECTIONS_PER_HOST, pool, ModelKeys.MAX_CONNECTIONS_PER_HOST);
                    this.writeOptional(writer, Attribute.MAX_TOTAL_CONNECTIONS, pool, ModelKeys.MAX_TOTAL_CONNECTIONS);
                    this.writeOptional(writer, Attribute.BUFFER_SIZE, pool, ModelKeys.BUFFER_SIZE);
                    this.writeOptional(writer, Attribute.SOCKET_TIMEOUT, pool, ModelKeys.SOCKET_TIMEOUT);
                    this.writeOptional(writer, Attribute.TCP_NO_DELAY, pool, ModelKeys.TCP_NO_DELAY);
                    writer.writeEndElement();
                }

                writer.writeEndElement();
            }
        }

        if (cache.get(ModelKeys.INDEXING).isDefined()|| cache.get(ModelKeys.INDEXING_PROPERTIES).isDefined()){
            writer.writeStartElement(Element.INDEXING.getLocalName());
            CacheConfigurationResource.INDEXING.marshallAsAttribute(cache, writer);
            CacheConfigurationResource.INDEXING_AUTO_CONFIG.marshallAsAttribute(cache, writer);
            CacheConfigurationResource.INDEXING_PROPERTIES.marshallAsElement(cache,writer);
            writer.writeEndElement();
        }

        if (cache.get(ModelKeys.STATE_TRANSFER, ModelKeys.STATE_TRANSFER_NAME).isDefined()) {
            ModelNode stateTransfer = cache.get(ModelKeys.STATE_TRANSFER, ModelKeys.STATE_TRANSFER_NAME);
            writer.writeStartElement(Element.STATE_TRANSFER.getLocalName());
            this.writeOptional(writer, Attribute.AWAIT_INITIAL_TRANSFER, stateTransfer, ModelKeys.AWAIT_INITIAL_TRANSFER);
            this.writeOptional(writer, Attribute.ENABLED, stateTransfer, ModelKeys.ENABLED);
            this.writeOptional(writer, Attribute.TIMEOUT, stateTransfer, ModelKeys.TIMEOUT);
            this.writeOptional(writer, Attribute.CHUNK_SIZE, stateTransfer, ModelKeys.CHUNK_SIZE);
            writer.writeEndElement();
        }

        if (cache.get(ModelKeys.PARTITION_HANDLING, ModelKeys.PARTITION_HANDLING_NAME).isDefined()) {
            ModelNode partitionHandling = cache.get(ModelKeys.PARTITION_HANDLING, ModelKeys.PARTITION_HANDLING_NAME);
            writer.writeStartElement(Element.PARTITION_HANDLING.getLocalName());
            this.writeOptional(writer, Attribute.ENABLED, partitionHandling, ModelKeys.ENABLED);
            writer.writeEndElement();
        }

    }

    private void writeListAsAttribute(XMLExtendedStreamWriter writer, Attribute attribute, ModelNode node, String key) throws XMLStreamException {
        if (node.hasDefined(key)) {
            StringBuilder result = new StringBuilder() ;
            ModelNode list = node.get(key);
            if (list.isDefined() && list.getType() == ModelType.LIST) {
                List<ModelNode> nodeList = list.asList();
                for (int i = 0; i < nodeList.size(); i++) {
                    result.append(nodeList.get(i).asString());
                    if (i < nodeList.size()-1) {
                        result.append(" ");
                    }
                }
                writer.writeAttribute(attribute.getLocalName(), result.toString());
            }
        }
    }

    private void writeJDBCStoreTable(XMLExtendedStreamWriter writer, Element element, ModelNode store, String key) throws XMLStreamException {
        if (store.hasDefined(key)) {
            ModelNode table = store.get(key);
            writer.writeStartElement(element.getLocalName());
            this.writeOptional(writer, Attribute.PREFIX, table, ModelKeys.PREFIX);
            this.writeOptional(writer, Attribute.BATCH_SIZE, table, ModelKeys.BATCH_SIZE);
            this.writeOptional(writer, Attribute.FETCH_SIZE, table, ModelKeys.FETCH_SIZE);
            this.writeOptional(writer, Attribute.CREATE_ON_START, table, ModelKeys.CREATE_ON_START);
            this.writeOptional(writer, Attribute.DROP_ON_EXIT, table, ModelKeys.DROP_ON_EXIT);
            this.writeJDBCStoreColumn(writer, Element.ID_COLUMN, table, ModelKeys.ID_COLUMN);
            this.writeJDBCStoreColumn(writer, Element.DATA_COLUMN, table, ModelKeys.DATA_COLUMN);
            this.writeJDBCStoreColumn(writer, Element.TIMESTAMP_COLUMN, table, ModelKeys.TIMESTAMP_COLUMN);
            writer.writeEndElement();
        }
    }

    private void writeJDBCStoreColumn(XMLExtendedStreamWriter writer, Element element, ModelNode table, String key) throws XMLStreamException {
        if (table.hasDefined(key)) {
            ModelNode column = table.get(key);
            writer.writeStartElement(element.getLocalName());
            this.writeOptional(writer, Attribute.NAME, column, ModelKeys.NAME);
            this.writeOptional(writer, Attribute.TYPE, column, ModelKeys.TYPE);
            writer.writeEndElement();
        }
    }

    private void writeLoaderAttributes(XMLExtendedStreamWriter writer, ModelNode store) throws XMLStreamException {
        this.writeOptional(writer, Attribute.SHARED, store, ModelKeys.SHARED);
        this.writeOptional(writer, Attribute.PRELOAD, store, ModelKeys.PRELOAD);
    }

    private void writeJdbcStoreAttributes(XMLExtendedStreamWriter writer, ModelNode store) throws XMLStreamException {
        this.writeRequired(writer, Attribute.DATASOURCE, store, ModelKeys.DATASOURCE);
        this.writeOptional(writer, Attribute.DIALECT, store, ModelKeys.DIALECT);
        this.writeStoreAttributes(writer, store);
    }

    private void writeStoreAttributes(XMLExtendedStreamWriter writer, ModelNode store) throws XMLStreamException {
        this.writeOptional(writer, Attribute.SHARED, store, ModelKeys.SHARED);
        this.writeOptional(writer, Attribute.PRELOAD, store, ModelKeys.PRELOAD);
        this.writeOptional(writer, Attribute.PASSIVATION, store, ModelKeys.PASSIVATION);
        this.writeOptional(writer, Attribute.FETCH_STATE, store, ModelKeys.FETCH_STATE);
        this.writeOptional(writer, Attribute.PURGE, store, ModelKeys.PURGE);
        this.writeOptional(writer, Attribute.READ_ONLY, store, ModelKeys.READ_ONLY);
        this.writeOptional(writer, Attribute.SINGLETON, store, ModelKeys.SINGLETON);
    }

    private void writeStoreWriteBehind(XMLExtendedStreamWriter writer, ModelNode store) throws XMLStreamException {
        if (store.get(ModelKeys.WRITE_BEHIND, ModelKeys.WRITE_BEHIND_NAME).isDefined()) {
            ModelNode writeBehind = store.get(ModelKeys.WRITE_BEHIND, ModelKeys.WRITE_BEHIND_NAME);
            writer.writeStartElement(Element.WRITE_BEHIND.getLocalName());
            this.writeOptional(writer, Attribute.FLUSH_LOCK_TIMEOUT, writeBehind, ModelKeys.FLUSH_LOCK_TIMEOUT);
            this.writeOptional(writer, Attribute.MODIFICATION_QUEUE_SIZE, writeBehind, ModelKeys.MODIFICATION_QUEUE_SIZE);
            this.writeOptional(writer, Attribute.SHUTDOWN_TIMEOUT, writeBehind, ModelKeys.SHUTDOWN_TIMEOUT);
            this.writeOptional(writer, Attribute.THREAD_POOL_SIZE, writeBehind, ModelKeys.THREAD_POOL_SIZE);
            writer.writeEndElement();
        }
    }

    private void writeStoreProperties(XMLExtendedStreamWriter writer, ModelNode store) throws XMLStreamException {
        if (store.hasDefined(ModelKeys.PROPERTY)) {
            // the format of the property elements
            //  "property" => {
            //       "relative-to" => {"value" => "fred"},
            //   }
            for (Property property: store.get(ModelKeys.PROPERTY).asPropertyList()) {
                writer.writeStartElement(Element.PROPERTY.getLocalName());
                writer.writeAttribute(Attribute.NAME.getLocalName(), property.getName());
                Property complexValue = property.getValue().asProperty();
                writer.writeCharacters(complexValue.getValue().asString());
                writer.writeEndElement();
            }
        }
    }

    private void writeStoreExpiration(XMLExtendedStreamWriter writer, ModelNode store) throws XMLStreamException {
        if (store.get(ModelKeys.EXPIRATION, ModelKeys.EXPIRATION_NAME).isDefined()) {
            ModelNode expiration = store.get(ModelKeys.EXPIRATION, ModelKeys.EXPIRATION_NAME);
            writer.writeStartElement(Element.EXPIRATION.getLocalName());
            this.writeOptional(writer, Attribute.PATH, expiration, ModelKeys.PATH);
            this.writeOptional(writer, Attribute.RELATIVE_TO, expiration, ModelKeys.RELATIVE_TO);
            this.writeOptional(writer, Attribute.QUEUE_SIZE, expiration, ModelKeys.QUEUE_SIZE);
            writer.writeEndElement();
        }
    }

    private void writeStoreCompression(XMLExtendedStreamWriter writer, ModelNode store) throws XMLStreamException {
        if (store.get(ModelKeys.COMPRESSION, ModelKeys.COMPRESSION_NAME).isDefined()) {
            ModelNode compression = store.get(ModelKeys.COMPRESSION, ModelKeys.COMPRESSION_NAME);
            writer.writeStartElement(Element.COMPRESSION.getLocalName());
            this.writeOptional(writer, Attribute.TYPE, compression, ModelKeys.TYPE);
            writer.writeEndElement();
        }
    }

    private void writeStoreImplementation(XMLExtendedStreamWriter writer, ModelNode store) throws XMLStreamException {
        if (store.get(ModelKeys.IMPLEMENTATION, ModelKeys.IMPLEMENTATION_NAME).isDefined()) {
            ModelNode implementation = store.get(ModelKeys.IMPLEMENTATION, ModelKeys.IMPLEMENTATION_NAME);
            writer.writeStartElement(Element.IMPLEMENTATION.getLocalName());
            this.writeOptional(writer, Attribute.TYPE, implementation, ModelKeys.IMPLEMENTATION);
            writer.writeEndElement();
        }
    }

    private void writeOptional(XMLExtendedStreamWriter writer, Attribute attribute, ModelNode model, String key) throws XMLStreamException {
        if (model.hasDefined(key)) {
            writer.writeAttribute(attribute.getLocalName(), model.get(key).asString());
        }
    }

    private void writeRequired(XMLExtendedStreamWriter writer, Attribute attribute, ModelNode model, String key) throws XMLStreamException {
        writer.writeAttribute(attribute.getLocalName(), model.require(key).asString());
    }

    private static boolean hasDefined(ModelNode model, Iterable<? extends AttributeDefinition> attributes) {
        for (AttributeDefinition attribute : attributes) {
            if (model.hasDefined(attribute.getName())) return true;
        }
        return false;
    }

    private static void writeAttributes(XMLExtendedStreamWriter writer, ModelNode model, Iterable<? extends AttributeDefinition> attributes) throws XMLStreamException {
        for (AttributeDefinition attribute : attributes) {
            writeAttribute(writer, model, attribute);
        }
    }

    private static void writeAttribute(XMLExtendedStreamWriter writer, ModelNode model, AttributeDefinition attribute) throws XMLStreamException {
        attribute.getAttributeMarshaller().marshallAsAttribute(attribute, model, true, writer);
    }
}
