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

import static org.jboss.as.clustering.infinispan.InfinispanLogger.ROOT_LOGGER;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.ADD;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.OP_ADDR;

import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import javax.xml.stream.Location;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;

import org.infinispan.security.impl.ClusterRoleMapper;
import org.infinispan.security.impl.CommonNameRoleMapper;
import org.infinispan.security.impl.IdentityRoleMapper;
import org.infinispan.server.jgroups.subsystem.ChannelResourceDefinition;
import org.infinispan.server.jgroups.subsystem.JGroupsSubsystemResourceDefinition;
import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.ObjectTypeAttributeDefinition;
import org.jboss.as.controller.PathAddress;
import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.descriptions.ModelDescriptionConstants;
import org.jboss.as.controller.operations.common.Util;
import org.jboss.as.controller.parsing.ParseUtils;
import org.jboss.dmr.ModelNode;
import org.jboss.logging.Logger;
import org.jboss.staxmapper.XMLElementReader;
import org.jboss.staxmapper.XMLExtendedStreamReader;

/**
 * Infinispan subsystem parsing code.
 *
 * @author Paul Ferraro
 * @author Richard Achmatowicz
 * @author Tristan Tarrant
 * @author Radoslav Husar
 * @author William Burns
 * @author Martin Gencur
 */
public final class InfinispanSubsystemXMLReader implements XMLElementReader<List<ModelNode>> {
   private static final Logger log = Logger.getLogger(InfinispanSubsystemXMLReader.class);
   private final Namespace namespace;

   public InfinispanSubsystemXMLReader(Namespace namespace) {
       this.namespace = namespace;
   }

    /**
     * {@inheritDoc}
     * @see org.jboss.staxmapper.XMLElementReader#readElement(org.jboss.staxmapper.XMLExtendedStreamReader, Object)
     */
    @Override
    public void readElement(XMLExtendedStreamReader reader, List<ModelNode> result) throws XMLStreamException {

        Map<PathAddress, ModelNode> operations = new LinkedHashMap<>();

        PathAddress subsystemAddress = PathAddress.pathAddress(InfinispanExtension.SUBSYSTEM_PATH);
        ModelNode subsystem = Util.createAddOperation(subsystemAddress);

        // command to add the subsystem
        operations.put(subsystemAddress, subsystem);

        while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
            Element element = Element.forName(reader.getLocalName());
            switch (element) {
                case CACHE_CONTAINER: {
                    parseContainer(reader, subsystemAddress, operations);
                    break;
                }
                default: {
                    throw ParseUtils.unexpectedElement(reader);
                }
            }
        }

        result.addAll(operations.values());
    }

    private void parseContainer(XMLExtendedStreamReader reader, PathAddress subsystemAddress, Map<PathAddress, ModelNode> operations) throws XMLStreamException {

        ModelNode container = Util.getEmptyOperation(ADD, null);
        String name = null;
        final Set<Attribute> required = EnumSet.of(Attribute.NAME);

        for (int i = 0; i < reader.getAttributeCount(); i++) {
            ParseUtils.requireNoNamespaceAttribute(reader, i);
            String value = reader.getAttributeValue(i);
            Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
            required.remove(attribute);
            switch (attribute) {
                case NAME: {
                    name = value;
                    break;
                }
                case ALIASES: {
                    for (String alias: reader.getListAttributeValue(i)) {
                        container.get(ModelKeys.ALIASES).add(alias);
                    }
                    break;
                }
                case DEFAULT_CACHE: {
                    CacheContainerResource.DEFAULT_CACHE.parseAndSetParameter(value, container, reader);
                    break;
                }
                case JNDI_NAME: {
                    CacheContainerResource.JNDI_NAME.parseAndSetParameter(value, container, reader);
                    break;
                }
                case START: {
                    CacheContainerResource.START.parseAndSetParameter(value, container, reader);
                    break;
                }
                case LISTENER_EXECUTOR: {
                    if (namespace.since(Namespace.INFINISPAN_SERVER_8_0)) {
                        throw ParseUtils.unexpectedAttribute(reader, i);
                    } else {
                        ROOT_LOGGER.deprecatedExecutor(ModelKeys.LISTENER_EXECUTOR, ModelKeys.LISTENER_THREAD_POOL);
                    }
                    break;
                }
                case ASYNC_EXECUTOR: {
                    if (namespace.since(Namespace.INFINISPAN_SERVER_8_0)) {
                        throw ParseUtils.unexpectedAttribute(reader, i);
                    } else {
                        ROOT_LOGGER.deprecatedExecutor(ModelKeys.ASYNC_EXECUTOR, ModelKeys.ASYNC_OPERATIONS_THREAD_POOL);
                    }
                    break;
                }
                case EVICTION_EXECUTOR: {
                    if (namespace.since(Namespace.INFINISPAN_SERVER_8_0)) {
                        throw ParseUtils.unexpectedAttribute(reader, i);
                    } else {
                        ROOT_LOGGER.deprecatedExecutor(ModelKeys.EVICTION_EXECUTOR, ModelKeys.EXPIRATION_THREAD_POOL);
                    }
                    break;
                }
                case EXPIRATION_EXECUTOR: {
                    if (namespace.since(Namespace.INFINISPAN_SERVER_8_0)) {
                        throw ParseUtils.unexpectedAttribute(reader, i);
                    } else {
                        ROOT_LOGGER.deprecatedExecutor(ModelKeys.EXPIRATION_EXECUTOR, ModelKeys.EXPIRATION_THREAD_POOL);
                    }
                   break;
                }
                case REPLICATION_QUEUE_EXECUTOR: {
                    if (namespace.since(Namespace.INFINISPAN_SERVER_8_0)) {
                        throw ParseUtils.unexpectedAttribute(reader, i);
                    } else {
                        ROOT_LOGGER.deprecatedExecutor(ModelKeys.REPLICATION_QUEUE_EXECUTOR, ModelKeys.REPLICATION_QUEUE_THREAD_POOL);
                    }
                    break;
                }
                case STATE_TRANSFER_EXECUTOR: {
                    if (namespace.since(Namespace.INFINISPAN_SERVER_8_0)) {
                        throw ParseUtils.unexpectedAttribute(reader, i);
                    } else {
                        ROOT_LOGGER.deprecatedExecutor(ModelKeys.STATE_TRANSFER_EXECUTOR, ModelKeys.STATE_TRANSFER_THREAD_POOL);
                    }
                    break;
                }
                case MODULE: {
                    CacheContainerResource.CACHE_CONTAINER_MODULE.parseAndSetParameter(value, container, reader);
                    break;
                }
                case STATISTICS: {
                   CacheContainerResource.STATISTICS.parseAndSetParameter(value, container, reader);
                   break;
                }
                default: {
                    throw ParseUtils.unexpectedAttribute(reader, i);
                }
            }
        }

        if (!required.isEmpty()) {
            throw ParseUtils.missingRequired(reader, required);
        }

        PathAddress containerAddress = subsystemAddress.append(ModelKeys.CACHE_CONTAINER, name);
        container.get(OP_ADDR).set(containerAddress.toModelNode());
        operations.put(containerAddress, container);

        PathAddress configurationsAddress = containerAddress.append(CacheContainerConfigurationsResource.PATH);
        operations.put(configurationsAddress, Util.getEmptyOperation(ADD, configurationsAddress.toModelNode()));

        Stream.of(ThreadPoolResource.values()).forEach(
                pool -> operations.put(containerAddress.append(pool.getPathElement()), Util.createAddOperation(containerAddress.append(pool.getPathElement())))
        );
        Stream.of(ScheduledThreadPoolResource.values()).forEach(
                pool -> operations.put(containerAddress.append(pool.getPathElement()), Util.createAddOperation(containerAddress.append(pool.getPathElement())))
        );

        while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
            Element element = Element.forName(reader.getLocalName());
            switch (element) {
                case TRANSPORT: {
                    parseTransport(reader, containerAddress, operations);
                    break;
                }
                case SECURITY: {
                    if (namespace.since(Namespace.INFINISPAN_SERVER_7_0)) {
                        parseGlobalSecurity(reader, containerAddress, operations);
                    } else {
                        throw ParseUtils.unexpectedElement(reader);
                    }
                    break;
                }
                case GLOBAL_STATE: {
                   if (namespace.since(Namespace.INFINISPAN_SERVER_8_1)) {
                      parseGlobalState(reader, containerAddress, operations);
                   } else {
                      throw ParseUtils.unexpectedElement(reader);
                   }
                   break;
                }
                case LOCAL_CACHE: {
                    parseLocalCache(reader, containerAddress, operations, false);
                    break;
                }
                case LOCAL_CACHE_CONFIGURATION: {
                    if (namespace.since(Namespace.INFINISPAN_SERVER_8_0)) {
                        parseLocalCache(reader, containerAddress, operations, true);
                    } else {
                        throw ParseUtils.unexpectedElement(reader);
                    }
                    break;
                }
                case INVALIDATION_CACHE: {
                    parseInvalidationCache(reader, containerAddress, operations, false);
                    break;
                }
                case INVALIDATION_CACHE_CONFIGURATION: {
                    if (namespace.since(Namespace.INFINISPAN_SERVER_8_0)) {
                        parseInvalidationCache(reader, containerAddress, operations, true);
                    } else {
                        throw ParseUtils.unexpectedElement(reader);
                    }
                    break;
                }
                case REPLICATED_CACHE: {
                    parseReplicatedCache(reader, containerAddress, operations, false);
                    break;
                }
                case REPLICATED_CACHE_CONFIGURATION: {
                    if (namespace.since(Namespace.INFINISPAN_SERVER_8_0)) {
                        parseReplicatedCache(reader, containerAddress, operations, true);
                    } else {
                        throw ParseUtils.unexpectedElement(reader);
                    }
                    break;
                }
                case DISTRIBUTED_CACHE: {
                    parseDistributedCache(reader, containerAddress, operations, false);
                    break;
                }
                case DISTRIBUTED_CACHE_CONFIGURATION: {
                    if (namespace.since(Namespace.INFINISPAN_SERVER_8_0)) {
                        parseDistributedCache(reader, containerAddress, operations, true);
                    } else {
                        throw ParseUtils.unexpectedElement(reader);
                    }
                    break;
                }
                case ASYNC_OPERATIONS_THREAD_POOL: {
                    if (namespace.since(Namespace.INFINISPAN_SERVER_8_0)) {
                        this.parseThreadPool(ThreadPoolResource.ASYNC_OPERATIONS, reader, containerAddress, operations);
                        break;
                    }
                }
                case EXPIRATION_THREAD_POOL: {
                    if (namespace.since(Namespace.INFINISPAN_SERVER_8_0)) {
                        this.parseScheduledThreadPool(ScheduledThreadPoolResource.EXPIRATION, reader, containerAddress, operations);
                        break;
                    }
                }
                case LISTENER_THREAD_POOL: {
                    if (namespace.since(Namespace.INFINISPAN_SERVER_8_0)) {
                        this.parseThreadPool(ThreadPoolResource.LISTENER, reader, containerAddress, operations);
                        break;
                    }
                }
                case PERSISTENCE_THREAD_POOL: {
                    if (namespace.since(Namespace.INFINISPAN_SERVER_8_0)) {
                        this.parseThreadPool(ThreadPoolResource.PERSISTENCE, reader, containerAddress, operations);
                        break;
                    }
                }
                case REMOTE_COMMAND_THREAD_POOL: {
                    if (namespace.since(Namespace.INFINISPAN_SERVER_8_0)) {
                        this.parseThreadPool(ThreadPoolResource.REMOTE_COMMAND, reader, containerAddress, operations);
                        break;
                    }
                }
                case REPLICATION_QUEUE_THREAD_POOL: {
                    if (namespace.since(Namespace.INFINISPAN_SERVER_8_0)) {
                        this.parseScheduledThreadPool(ScheduledThreadPoolResource.REPLICATION_QUEUE, reader, containerAddress, operations);
                        break;
                    }
                }
                case STATE_TRANSFER_THREAD_POOL: {
                    if (namespace.since(Namespace.INFINISPAN_SERVER_8_0)) {
                        this.parseThreadPool(ThreadPoolResource.STATE_TRANSFER, reader, containerAddress, operations);
                        break;
                    }
                }
                case TRANSPORT_THREAD_POOL: {
                    if (namespace.since(Namespace.INFINISPAN_SERVER_8_0)) {
                        this.parseThreadPool(ThreadPoolResource.TRANSPORT, reader, containerAddress, operations);
                        break;
                    }
                }

                default: {
                    throw ParseUtils.unexpectedElement(reader);
                }
            }
        }
    }

    private void parseGlobalState(XMLExtendedStreamReader reader, PathAddress containerAddress,
            Map<PathAddress, ModelNode> operations) throws XMLStreamException {
        PathAddress globalStateAddress = containerAddress.append(ModelKeys.GLOBAL_STATE, ModelKeys.GLOBAL_STATE_NAME);
        ModelNode globalState = Util.createAddOperation(globalStateAddress);
        while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
            Element element = Element.forName(reader.getLocalName());
            switch (element) {
                case PERSISTENT_LOCATION: {
                    parseGlobalStatePath(reader, globalState, GlobalStateResource.PERSISTENT_LOCATION_PATH);
                    break;
                }
                case TEMPORARY_LOCATION: {
                    parseGlobalStatePath(reader, globalState, GlobalStateResource.TEMPORARY_STATE_PATH);
                    break;
                }
                default: {
                    throw ParseUtils.unexpectedElement(reader);
                }
            }
        }

        operations.put(globalStateAddress, globalState);
    }

    private void parseGlobalStatePath(XMLExtendedStreamReader reader, ModelNode operation,
            ObjectTypeAttributeDefinition statePath) throws XMLStreamException {
        ModelNode model = operation.get(statePath.getName());
        for (int i = 0; i < reader.getAttributeCount(); i++) {
            String value = reader.getAttributeValue(i);
            Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
            switch (attribute) {
                case RELATIVE_TO: {
                    GlobalStateResource.TEMPORARY_RELATIVE_TO.parseAndSetParameter(value, model, reader);
                    break;
                }
                case PATH: {
                    GlobalStateResource.PATH.parseAndSetParameter(value, model, reader);
                    break;
                }
                default: {
                    throw ParseUtils.unexpectedAttribute(reader, i);
                }
            }
        }

        ParseUtils.requireNoContent(reader);
    }

    private void parseTransport(XMLExtendedStreamReader reader, PathAddress containerAddress, Map<PathAddress, ModelNode> operations) throws XMLStreamException {

        PathAddress transportAddress = containerAddress.append(ModelKeys.TRANSPORT, ModelKeys.TRANSPORT_NAME);
        ModelNode transport = Util.createAddOperation(transportAddress);

        String stack = null;
        String cluster = null;

        for (int i = 0; i < reader.getAttributeCount(); i++) {
            String value = reader.getAttributeValue(i);
            Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
            switch (attribute) {
                case CHANNEL: {
                    TransportResource.CHANNEL.parseAndSetParameter(value, transport, reader);
                    break;
                }
                case EXECUTOR: {
                    if (namespace.since(Namespace.INFINISPAN_SERVER_8_0)) {
                        throw ParseUtils.unexpectedAttribute(reader, i);
                    } else {
                        ROOT_LOGGER.deprecatedExecutor(ModelKeys.EXECUTOR, ModelKeys.TRANSPORT_THREAD_POOL);
                    }
                    break;
                }
                case LOCK_TIMEOUT: {
                    TransportResource.LOCK_TIMEOUT.parseAndSetParameter(value, transport, reader);
                    break;
                }
                case REMOTE_COMMAND_EXECUTOR: {
                    if (namespace.since(Namespace.INFINISPAN_SERVER_8_0)) {
                        throw ParseUtils.unexpectedAttribute(reader, i);
                    } else {
                        ROOT_LOGGER.deprecatedExecutor(ModelKeys.REMOTE_COMMAND_EXECUTOR, ModelKeys.REMOTE_COMMAND_THREAD_POOL);
                    }
                    break;
                }
                case STRICT_PEER_TO_PEER: {
                    TransportResource.STRICT_PEER_TO_PEER.parseAndSetParameter(value, transport, reader);
                    break;
                }
                case TOTAL_ORDER_EXECUTOR: {
                    if (!namespace.since(Namespace.INFINISPAN_SERVER_8_0)) {
                        log.warn("The xml element total-order-executor has been removed and has no effect, please update your configuration file.");
                        break;
                    }
                }
                case STACK: {
                    if (!namespace.since(Namespace.INFINISPAN_SERVER_8_0)) {
                        stack = value;
                        break;
                    }
                }
                case CLUSTER: {
                    if (!namespace.since(Namespace.INFINISPAN_SERVER_8_0)) {
                        cluster = value;
                        break;
                    }
                }
                default: {
                    throw ParseUtils.unexpectedAttribute(reader, i);
                }
            }
        }

        if (!namespace.since(Namespace.INFINISPAN_SERVER_8_0)) {
            // We need to create a corresponding channel add operation
            String channel = (cluster != null) ? cluster : ("cluster-" + containerAddress.getLastElement().getValue());
            TransportResource.CHANNEL.parseAndSetParameter(channel, transport, reader);
            PathAddress channelAddress = PathAddress.pathAddress(JGroupsSubsystemResourceDefinition.PATH, ChannelResourceDefinition.pathElement(channel));
            ModelNode channelOperation = Util.createAddOperation(channelAddress);
            if (stack != null) {
                ChannelResourceDefinition.STACK.parseAndSetParameter(stack, channelOperation, reader);
            }
            operations.put(channelAddress, channelOperation);
        }


        ParseUtils.requireNoContent(reader);

        operations.put(transportAddress, transport);
    }

    private void parseGlobalSecurity(XMLExtendedStreamReader reader, PathAddress containerAddress, Map<PathAddress, ModelNode> operations) throws XMLStreamException {
        PathAddress securityAddress = containerAddress.append(ModelKeys.SECURITY, ModelKeys.SECURITY_NAME);
        ModelNode security = Util.createAddOperation(securityAddress);

        ParseUtils.requireNoAttributes(reader);

        Map<PathAddress, ModelNode> additionalConfigurationOperations = new LinkedHashMap<>();

        while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
            Element element = Element.forName(reader.getLocalName());
            switch (element) {
                case AUTHORIZATION: {
                    this.parseGlobalAuthorization(reader, securityAddress, additionalConfigurationOperations);
                    break;
                }
                default: {
                    throw ParseUtils.unexpectedElement(reader);
                }
            }
        }

        operations.put(securityAddress, security);
        operations.putAll(additionalConfigurationOperations);
    }

    private void parseGlobalAuthorization(XMLExtendedStreamReader reader, PathAddress securityAddress, Map<PathAddress, ModelNode> operations) throws XMLStreamException {
        PathAddress authorizationAddress = securityAddress.append(ModelKeys.AUTHORIZATION, ModelKeys.AUTHORIZATION_NAME);
        ModelNode authorization = Util.createAddOperation(authorizationAddress);

        String auditLogger = null;
        for (int i = 0; i < reader.getAttributeCount(); i++) {
            Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
            switch (attribute) {
                case AUDIT_LOGGER: {
                    if (auditLogger != null) {
                       throw ParseUtils.unexpectedElement(reader);
                    }
                    auditLogger = reader.getAttributeValue(i);
                    break;
                }
                default: {
                    throw ParseUtils.unexpectedAttribute(reader, i);
                }
            }
        }
        CacheContainerAuthorizationResource.AUDIT_LOGGER.parseAndSetParameter(auditLogger, authorization, reader);

        Map<PathAddress, ModelNode> additionalConfigurationOperations = new LinkedHashMap<>();
        String roleMapper = null;
        while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
            Element element = Element.forName(reader.getLocalName());
            switch (element) {
                case IDENTITY_ROLE_MAPPER:
                    if (roleMapper != null) {
                       throw ParseUtils.unexpectedElement(reader);
                    }
                    ParseUtils.requireNoAttributes(reader);
                    ParseUtils.requireNoContent(reader);
                    roleMapper = IdentityRoleMapper.class.getName();
                    break;
                 case COMMON_NAME_ROLE_MAPPER:
                    if (roleMapper != null) {
                       throw ParseUtils.unexpectedElement(reader);
                    }
                    ParseUtils.requireNoAttributes(reader);
                    ParseUtils.requireNoContent(reader);
                    roleMapper = CommonNameRoleMapper.class.getName();
                    break;
                 case CLUSTER_ROLE_MAPPER:
                    if (roleMapper != null) {
                       throw ParseUtils.unexpectedElement(reader);
                    }
                    ParseUtils.requireNoAttributes(reader);
                    ParseUtils.requireNoContent(reader);
                    roleMapper = ClusterRoleMapper.class.getName();
                    break;
                 case CUSTOM_ROLE_MAPPER:
                    if (roleMapper != null) {
                       throw ParseUtils.unexpectedElement(reader);
                    }
                    roleMapper = ParseUtils.readStringAttributeElement(reader, Attribute.CLASS.getLocalName());
                    break;
                case ROLE: {
                    this.parseGlobalRole(reader, authorizationAddress, additionalConfigurationOperations);
                    break;
                }
                default: {
                    throw ParseUtils.unexpectedElement(reader);
                }
            }
        }
        CacheContainerAuthorizationResource.MAPPER.parseAndSetParameter(roleMapper, authorization, reader);

        operations.put(authorizationAddress, authorization);
        operations.putAll(additionalConfigurationOperations);
    }

    private void parseGlobalRole(XMLExtendedStreamReader reader, PathAddress authorizationAddress, Map<PathAddress, ModelNode> operations) throws XMLStreamException {
        String[] attributes = ParseUtils.requireAttributes(reader, Attribute.NAME.getLocalName(), Attribute.PERMISSIONS.getLocalName());
        String name = attributes[0];
        PathAddress roleAddress = authorizationAddress.append(ModelKeys.ROLE, name);
        ModelNode role = Util.createAddOperation(roleAddress);
        AuthorizationRoleResource.NAME.parseAndSetParameter(name, role, reader);
        for(String perm : attributes[1].split("\\s+")) {
            AuthorizationRoleResource.PERMISSIONS.parseAndAddParameterElement(perm, role, reader);
        }

        ParseUtils.requireNoContent(reader);
        operations.put(roleAddress, role);
    }

    private void parseCacheAttribute(XMLExtendedStreamReader reader, int index, Attribute attribute, String value, ModelNode cache) throws XMLStreamException {
        switch (attribute) {
            case NAME: {
                CacheConfigurationResource.NAME.parseAndSetParameter(value, cache, reader);
                break;
            }
            case START: {
                CacheConfigurationResource.START.parseAndSetParameter(value, cache, reader);
                if (!value.equalsIgnoreCase("EAGER")) {
                   Location location = reader.getLocation();
                   log.warnf("Ignoring start mode [%s] at [row,col] [%s, %s], as EAGER is the only supported mode", value,
                         location.getLineNumber(), location.getColumnNumber());
                   cache.get(CacheConfigurationResource.START.getName()).set("EAGER");
                }
                break;
            }
            case CONFIGURATION: {
                CacheConfigurationResource.CONFIGURATION.parseAndSetParameter(value, cache, reader);
                break;
            }
            case JNDI_NAME: {
                CacheConfigurationResource.JNDI_NAME.parseAndSetParameter(value, cache, reader);
                break;
            }
            case BATCHING: {
                CacheConfigurationResource.BATCHING.parseAndSetParameter(value, cache, reader);
                break;
            }
            case MODULE: {
                CacheConfigurationResource.CACHE_MODULE.parseAndSetParameter(value, cache, reader);
                break;
            }
            case INLINE_INTERCEPTORS: {
                CacheConfigurationResource.INLINE_INTERCEPTORS.parseAndSetParameter(value, cache, reader);
                break;
            }
            case SIMPLE_CACHE: {
                CacheConfigurationResource.SIMPLE_CACHE.parseAndSetParameter(value, cache, reader);
                break;
            }
            case STATISTICS: {
                CacheConfigurationResource.STATISTICS.parseAndSetParameter(value, cache, reader);
                break;
            }
            case STATISTICS_AVAILABLE: {
                CacheConfigurationResource.STATISTICS_AVAILABLE.parseAndSetParameter(value, cache, reader);
                break;
            }
            default: {
                throw ParseUtils.unexpectedAttribute(reader, index);
            }
        }
    }

    private void parseClusteredCacheAttribute(XMLExtendedStreamReader reader, int index, Attribute attribute, String value, ModelNode cache) throws XMLStreamException {
        switch (attribute) {
            case ASYNC_MARSHALLING: {
                if (namespace.since(Namespace.INFINISPAN_SERVER_8_0)) {
                    throw ParseUtils.unexpectedAttribute(reader, index);
                } else {
                    log.warn("The async-marshalling attribute has been deprecated and has no effect, please update your configuration file.");
                    break;
                }
            }
            case MODE: {
                // note the use of ClusteredCacheAdd.MODE
                ClusteredCacheConfigurationResource.MODE.parseAndSetParameter(value, cache, reader);
                break;
            }
            case QUEUE_SIZE: {
                ClusteredCacheConfigurationResource.QUEUE_SIZE.parseAndSetParameter(value, cache, reader);
                break;
            }
            case QUEUE_FLUSH_INTERVAL: {
                ClusteredCacheConfigurationResource.QUEUE_FLUSH_INTERVAL.parseAndSetParameter(value, cache, reader);
                break;
            }
            case REMOTE_TIMEOUT: {
                ClusteredCacheConfigurationResource.REMOTE_TIMEOUT.parseAndSetParameter(value, cache, reader);
                break;
            }
            default: {
                this.parseCacheAttribute(reader, index, attribute, value, cache);
            }
        }
    }

    private void addCacheForConfiguration(ModelNode cacheConfiguration, String configurationName, PathAddress containerAddress, String type, Map<PathAddress, ModelNode> operations) {
        String name = PathAddress.pathAddress(cacheConfiguration.get(OP_ADDR)).getLastElement().getValue();
        PathAddress cacheAddress = containerAddress.append(type, name);
        ModelNode cache = Util.getEmptyOperation(ADD, cacheAddress.toModelNode());
        cache.get(ModelKeys.CONFIGURATION).set(configurationName == null ? name : configurationName);
        operations.put(cacheAddress, cache);
    }


    private void parseLocalCache(XMLExtendedStreamReader reader, PathAddress containerAddress, Map<PathAddress, ModelNode> operations, boolean configurationOnly) throws XMLStreamException {

        // ModelNode for the cache add operation
        ModelNode cacheConfiguration = Util.getEmptyOperation(ADD, null);
        // NOTE: this list is used to avoid lost attribute updates to the cache
        // object once it has been added to the operations list
        Map<PathAddress, ModelNode> additionalConfigurationOperations = new LinkedHashMap<>();

        for (int i = 0; i < reader.getAttributeCount(); i++) {
            String value = reader.getAttributeValue(i);
            Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
            this.parseCacheAttribute(reader, i, attribute, value, cacheConfiguration);
        }

        if (!cacheConfiguration.hasDefined(ModelKeys.NAME)) {
            throw ParseUtils.missingRequired(reader, EnumSet.of(Attribute.NAME));
        }

        // update the cache configuration address with the cache name
        PathAddress cacheConfigurationAddress = addNameToAddress(cacheConfiguration, containerAddress, ModelKeys.LOCAL_CACHE) ;

        while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
            Element element = Element.forName(reader.getLocalName());
            this.parseCacheElement(reader, element, cacheConfiguration, additionalConfigurationOperations);
        }
        addCacheConfiguration(ModelKeys.LOCAL_CACHE, containerAddress, operations, configurationOnly, cacheConfiguration,
                additionalConfigurationOperations, cacheConfigurationAddress);
    }

    private void addCacheConfiguration(String cacheType, PathAddress containerAddress, Map<PathAddress, ModelNode> operations,
            boolean configurationOnly, ModelNode cacheConfiguration,
            Map<PathAddress, ModelNode> additionalConfigurationOperations, PathAddress cacheConfigurationAddress) {
        if (configurationOnly) {
            // just create the configuration
            operations.put(cacheConfigurationAddress, cacheConfiguration);
            operations.putAll(additionalConfigurationOperations);
        } else {
            if (cacheConfiguration.hasDefined(ModelKeys.CONFIGURATION) && additionalConfigurationOperations.size() == 0) {
                // Pure instance
                addCacheForConfiguration(cacheConfiguration, cacheConfiguration.get(ModelKeys.CONFIGURATION).asString(), containerAddress, cacheType, operations);
            } else {
                operations.put(cacheConfigurationAddress, cacheConfiguration);
                operations.putAll(additionalConfigurationOperations);
                addCacheForConfiguration(cacheConfiguration, null, containerAddress, cacheType, operations);
            }
        }
    }

    private void parseDistributedCache(XMLExtendedStreamReader reader, PathAddress containerAddress, Map<PathAddress, ModelNode> operations, boolean configurationOnly) throws XMLStreamException {

        // ModelNode for the cache add operation
        ModelNode cacheConfiguration = Util.getEmptyOperation(ModelDescriptionConstants.ADD, null);
        Map<PathAddress, ModelNode> additionalConfigurationOperations = new LinkedHashMap<>();

        for (int i = 0; i < reader.getAttributeCount(); i++) {
            String value = reader.getAttributeValue(i);
            Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
            switch (attribute) {
                case OWNERS: {
                    DistributedCacheConfigurationResource.OWNERS.parseAndSetParameter(value, cacheConfiguration, reader);
                    break;
                }
                case SEGMENTS: {
                    DistributedCacheConfigurationResource.SEGMENTS.parseAndSetParameter(value, cacheConfiguration, reader);
                    break;
                }
               case CAPACITY_FACTOR: {
                   DistributedCacheConfigurationResource.CAPACITY_FACTOR.parseAndSetParameter(value, cacheConfiguration, reader);
                  break;
               }
                case L1_LIFESPAN: {
                    DistributedCacheConfigurationResource.L1_LIFESPAN.parseAndSetParameter(value, cacheConfiguration, reader);
                    break;
                }
                default: {
                    this.parseClusteredCacheAttribute(reader, i, attribute, value, cacheConfiguration);
                }
            }
        }

        if (!cacheConfiguration.hasDefined(ModelKeys.NAME)) {
            throw ParseUtils.missingRequired(reader, EnumSet.of(Attribute.NAME));
        }
        if (!cacheConfiguration.hasDefined(ModelKeys.MODE) && !cacheConfiguration.hasDefined(ModelKeys.CONFIGURATION)) {
            throw ParseUtils.missingRequired(reader, EnumSet.of(Attribute.MODE));
        }

        // update the cache address with the cache name
        PathAddress cacheConfigurationAddress = addNameToAddress(cacheConfiguration, containerAddress, ModelKeys.DISTRIBUTED_CACHE) ;

        while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
            Element element = Element.forName(reader.getLocalName());
            switch (element) {
                case PARTITION_HANDLING: {
                    if (namespace.since(Namespace.INFINISPAN_SERVER_7_0)) {
                        this.parsePartitionHandling(reader, cacheConfiguration, additionalConfigurationOperations);
                    } else {
                        throw ParseUtils.unexpectedElement(reader);
                    }
                    break;
                }
                case STATE_TRANSFER: {
                    this.parseStateTransfer(reader, cacheConfiguration, additionalConfigurationOperations);
                    break;
                }
                default: {
                    this.parseCacheElement(reader, element, cacheConfiguration, additionalConfigurationOperations);
                }
            }
        }

        addCacheConfiguration(ModelKeys.DISTRIBUTED_CACHE, containerAddress, operations, configurationOnly, cacheConfiguration,
                additionalConfigurationOperations, cacheConfigurationAddress);
    }

    private void parseReplicatedCache(XMLExtendedStreamReader reader, PathAddress containerAddress, Map<PathAddress, ModelNode> operations, boolean configurationOnly) throws XMLStreamException {

        // ModelNode for the cache add operation
        ModelNode cacheConfiguration = Util.getEmptyOperation(ModelDescriptionConstants.ADD, null);
        Map<PathAddress, ModelNode> additionalConfigurationOperations = new LinkedHashMap<>();

        for (int i = 0; i < reader.getAttributeCount(); i++) {
            String value = reader.getAttributeValue(i);
            Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
            this.parseClusteredCacheAttribute(reader, i, attribute, value, cacheConfiguration);
        }

        if (!cacheConfiguration.hasDefined(ModelKeys.NAME)) {
            throw ParseUtils.missingRequired(reader, EnumSet.of(Attribute.NAME));
        }
        if (!cacheConfiguration.hasDefined(ModelKeys.MODE) && !cacheConfiguration.hasDefined(ModelKeys.CONFIGURATION)) {
            throw ParseUtils.missingRequired(reader, EnumSet.of(Attribute.MODE));
        }

        // update the cache address with the cache name
        PathAddress cacheConfigurationAddress = addNameToAddress(cacheConfiguration, containerAddress, ModelKeys.REPLICATED_CACHE) ;

        while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
            Element element = Element.forName(reader.getLocalName());
            switch (element) {
                case PARTITION_HANDLING: {
                    this.parsePartitionHandling(reader, cacheConfiguration, additionalConfigurationOperations);
                    break;
                }
                case STATE_TRANSFER: {
                    this.parseStateTransfer(reader, cacheConfiguration, additionalConfigurationOperations);
                    break;
                }
                default: {
                    this.parseCacheElement(reader, element, cacheConfiguration, additionalConfigurationOperations);
                }
            }
        }

        addCacheConfiguration(ModelKeys.REPLICATED_CACHE, containerAddress, operations, configurationOnly, cacheConfiguration,
                additionalConfigurationOperations, cacheConfigurationAddress);
    }

    private void parseInvalidationCache(XMLExtendedStreamReader reader, PathAddress containerAddress, Map<PathAddress, ModelNode> operations, boolean configurationOnly) throws XMLStreamException {

        // ModelNode for the cache add operation
        ModelNode cacheConfiguration = Util.getEmptyOperation(ModelDescriptionConstants.ADD, null);
        Map<PathAddress, ModelNode> additionalConfigurationOperations = new LinkedHashMap<>();

        for (int i = 0; i < reader.getAttributeCount(); i++) {
            String value = reader.getAttributeValue(i);
            Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
            this.parseClusteredCacheAttribute(reader, i, attribute, value, cacheConfiguration);
        }

        if (!cacheConfiguration.hasDefined(ModelKeys.NAME)) {
            throw ParseUtils.missingRequired(reader, EnumSet.of(Attribute.NAME));
        }
        if (!cacheConfiguration.hasDefined(ModelKeys.MODE) && !cacheConfiguration.hasDefined(ModelKeys.CONFIGURATION)) {
            throw ParseUtils.missingRequired(reader, EnumSet.of(Attribute.MODE));
        }

        // update the cache address with the cache name
        PathAddress cacheConfigurationAddress = addNameToAddress(cacheConfiguration, containerAddress, ModelKeys.INVALIDATION_CACHE) ;

        while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
            Element element = Element.forName(reader.getLocalName());
            switch (element) {
                default: {
                    this.parseCacheElement(reader, element, cacheConfiguration, additionalConfigurationOperations);
                }
            }
        }

        addCacheConfiguration(ModelKeys.INVALIDATION_CACHE, containerAddress, operations, configurationOnly, cacheConfiguration,
                additionalConfigurationOperations, cacheConfigurationAddress);
    }

    private PathAddress addNameToAddress(ModelNode current, PathAddress containerAddress, String type) {
        String name = current.get(ModelKeys.NAME).asString();
        // setup the cache configuration address
        PathAddress cacheConfigurationAddress = containerAddress.append(ModelKeys.CONFIGURATIONS, ModelKeys.CONFIGURATIONS_NAME).append(type + ModelKeys.CONFIGURATION_SUFFIX, name);
        current.get(ModelDescriptionConstants.OP_ADDR).set(cacheConfigurationAddress.toModelNode());
        // get rid of NAME now that we are finished with it
        current.remove(ModelKeys.NAME);
        return cacheConfigurationAddress;
    }

    private void parseCacheElement(XMLExtendedStreamReader reader, Element element, ModelNode cache, Map<PathAddress, ModelNode> operations) throws XMLStreamException {
        switch (element) {
            case BACKUPS: {
                this.parseBackups(reader, cache, operations);
                break;
            }
            case BACKUP_FOR: {
                if (namespace.since(Namespace.INFINISPAN_SERVER_7_0)) {
                    this.parseBackupFor(reader, cache);
                } else {
                    throw ParseUtils.unexpectedElement(reader);
                }
                break;
            }
            case CLUSTER_LOADER: {
                this.parseClusterLoader(reader, cache, operations);
                break;
            }
            case COMPATIBILITY: {
                this.parseCompatibility(reader, cache, operations);
                break;
            }
            case LOCKING: {
                this.parseLocking(reader, cache, operations);
                break;
            }
            case TRANSACTION: {
                this.parseTransaction(reader, cache, operations);
                break;
            }
            case EVICTION: {
                this.parseEviction(reader, cache, operations);
                break;
            }
            case EXPIRATION: {
                this.parseExpiration(reader, cache, operations);
                break;
            }
            case LOADER: {
                this.parseCustomLoader(reader, cache, operations);
                break;
            }
            case STORE: {
                this.parseCustomStore(reader, cache, operations);
                break;
            }
            case FILE_STORE: {
                this.parseFileStore(reader, cache, operations);
                break;
            }
            case STRING_KEYED_JDBC_STORE: {
                this.parseStringKeyedJDBCStore(reader, cache, operations);
                break;
            }
            case BINARY_KEYED_JDBC_STORE: {
                this.parseBinaryKeyedJDBCStore(reader, cache, operations);
                break;
            }
            case MIXED_KEYED_JDBC_STORE: {
                this.parseMixedKeyedJDBCStore(reader, cache, operations);
                break;
            }
            case REMOTE_STORE: {
                this.parseRemoteStore(reader, cache, operations);
                break;
            }
            case LEVELDB_STORE: {
                this.parseLevelDBStore(reader, cache, operations);
                break;
            }
            case REST_STORE: {
                this.parseRestStore(reader, cache, operations);
                break;
            }
            case INDEXING: {
                this.parseIndexing(reader, cache);
                break;
            }
            case SECURITY: {
                if (namespace.since(Namespace.INFINISPAN_SERVER_7_0)) {
                    this.parseCacheSecurity(reader, cache, operations);
                    break;
                }
            }
            default: {
                throw ParseUtils.unexpectedElement(reader);
            }
        }
    }

    private void parseStateTransfer(XMLExtendedStreamReader reader, ModelNode cache, Map<PathAddress, ModelNode> operations) throws XMLStreamException {

        PathAddress stateTransferAddress = PathAddress.pathAddress(cache.get(OP_ADDR)).append(ModelKeys.STATE_TRANSFER, ModelKeys.STATE_TRANSFER_NAME);
        ModelNode stateTransfer = Util.createAddOperation(stateTransferAddress);

        for (int i = 0; i < reader.getAttributeCount(); i++) {
            String value = reader.getAttributeValue(i);
            Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
            switch (attribute) {
                case AWAIT_INITIAL_TRANSFER: {
                    StateTransferConfigurationResource.AWAIT_INITIAL_TRANSFER.parseAndSetParameter(value, stateTransfer, reader);
                    break;
                }
                case ENABLED: {
                    StateTransferConfigurationResource.ENABLED.parseAndSetParameter(value, stateTransfer, reader);
                    break;
                }
                case TIMEOUT: {
                    StateTransferConfigurationResource.TIMEOUT.parseAndSetParameter(value, stateTransfer, reader);
                    break;
                }
                case CHUNK_SIZE: {
                    StateTransferConfigurationResource.CHUNK_SIZE.parseAndSetParameter(value, stateTransfer, reader);
                    break;
                }
                default: {
                    throw ParseUtils.unexpectedAttribute(reader, i);
                }
            }
        }
        ParseUtils.requireNoContent(reader);
        operations.put(stateTransferAddress, stateTransfer);
    }

    private void parseLocking(XMLExtendedStreamReader reader, ModelNode cache, Map<PathAddress, ModelNode> operations) throws XMLStreamException {

        PathAddress lockingAddress = PathAddress.pathAddress(cache.get(OP_ADDR)).append(ModelKeys.LOCKING, ModelKeys.LOCKING_NAME);
        ModelNode locking = Util.createAddOperation(lockingAddress);

        for (int i = 0; i < reader.getAttributeCount(); i++) {
            String value = reader.getAttributeValue(i);
            Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
            switch (attribute) {
                case ISOLATION: {
                    LockingConfigurationResource.ISOLATION.parseAndSetParameter(value, locking, reader);
                    break;
                }
                case STRIPING: {
                    LockingConfigurationResource.STRIPING.parseAndSetParameter(value, locking, reader);
                    break;
                }
                case ACQUIRE_TIMEOUT: {
                    LockingConfigurationResource.ACQUIRE_TIMEOUT.parseAndSetParameter(value, locking, reader);
                    break;
                }
                case CONCURRENCY_LEVEL: {
                    LockingConfigurationResource.CONCURRENCY_LEVEL.parseAndSetParameter(value, locking, reader);
                    break;
                }
                default: {
                    throw ParseUtils.unexpectedAttribute(reader, i);
                }
            }
        }
        ParseUtils.requireNoContent(reader);
        operations.put(lockingAddress, locking);
    }

    private void parseTransaction(XMLExtendedStreamReader reader, ModelNode cache, Map<PathAddress, ModelNode> operations) throws XMLStreamException {

        PathAddress transactionAddress = PathAddress.pathAddress(cache.get(OP_ADDR)).append(TransactionConfigurationResource.PATH);
        ModelNode transaction = Util.createAddOperation(transactionAddress);

        for (int i = 0; i < reader.getAttributeCount(); i++) {
            String value = reader.getAttributeValue(i);
            Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
            switch (attribute) {
                case STOP_TIMEOUT: {
                    TransactionConfigurationResource.STOP_TIMEOUT.parseAndSetParameter(value, transaction, reader);
                    break;
                }
                case MODE: {
                    TransactionConfigurationResource.MODE.parseAndSetParameter(value, transaction, reader);
                    break;
                }
                case LOCKING: {
                    TransactionConfigurationResource.LOCKING.parseAndSetParameter(value, transaction, reader);
                    break;
                }
                case NOTIFICATIONS: {
                    TransactionConfigurationResource.NOTIFICATIONS.parseAndSetParameter(value, transaction, reader);
                    break;
                }
                default: {
                    throw ParseUtils.unexpectedAttribute(reader, i);
                }
            }
        }
        ParseUtils.requireNoContent(reader);
        operations.put(transactionAddress, transaction);
    }

    private void parseEviction(XMLExtendedStreamReader reader, ModelNode cache, Map<PathAddress, ModelNode> operations) throws XMLStreamException {

        PathAddress evictionAddress = PathAddress.pathAddress(cache.get(OP_ADDR)).append(EvictionConfigurationResource.PATH);
        ModelNode eviction = Util.createAddOperation(evictionAddress);

        for (int i = 0; i < reader.getAttributeCount(); i++) {
            String value = reader.getAttributeValue(i);
            Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
            switch (attribute) {
                case STRATEGY: {
                    EvictionConfigurationResource.EVICTION_STRATEGY.parseAndSetParameter(value, eviction, reader);
                    break;
                }
                case MAX_ENTRIES: {
                    if (namespace.since(Namespace.INFINISPAN_SERVER_8_1)) {
                        throw ParseUtils.unexpectedAttribute(reader, i);
                    } else {
                        EvictionConfigurationResource.SIZE.parseAndSetParameter(value, eviction, reader);
                    }
                    break;
                }
                case TYPE: {
                    EvictionConfigurationResource.TYPE.parseAndSetParameter(value, eviction, reader);
                    break;
                }
                case SIZE: {
                    EvictionConfigurationResource.SIZE.parseAndSetParameter(value, eviction, reader);
                    break;
                }
                default: {
                    throw ParseUtils.unexpectedAttribute(reader, i);
                }
            }
        }
        ParseUtils.requireNoContent(reader);
        operations.put(evictionAddress, eviction);
    }

    private void parseExpiration(XMLExtendedStreamReader reader, ModelNode cache, Map<PathAddress, ModelNode> operations) throws XMLStreamException {

        PathAddress expirationAddress = PathAddress.pathAddress(cache.get(OP_ADDR)).append(ExpirationConfigurationResource.PATH);
        ModelNode expiration = Util.createAddOperation(expirationAddress);

        for (int i = 0; i < reader.getAttributeCount(); i++) {
            String value = reader.getAttributeValue(i);
            Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
            switch (attribute) {
                case MAX_IDLE: {
                    ExpirationConfigurationResource.MAX_IDLE.parseAndSetParameter(value, expiration, reader);
                    break;
                }
                case LIFESPAN: {
                    ExpirationConfigurationResource.LIFESPAN.parseAndSetParameter(value, expiration, reader);
                    break;
                }
                case INTERVAL: {
                    ExpirationConfigurationResource.INTERVAL.parseAndSetParameter(value, expiration, reader);
                    break;
                }
                default: {
                    throw ParseUtils.unexpectedAttribute(reader, i);
                }
            }
        }
        ParseUtils.requireNoContent(reader);
        operations.put(expirationAddress, expiration);
    }

    private PathAddress setOperationAddress(ModelNode operation, PathAddress address, PathElement element, String name) {
        address = address.append(element.getKey(), name);
        operation.get(ModelDescriptionConstants.OP_ADDR).set(address.toModelNode());
        if (operation.hasDefined(ModelKeys.NAME))
            operation.remove(ModelKeys.NAME);
        return address;
    }

    private void parseCustomLoader(XMLExtendedStreamReader reader, ModelNode cache, Map<PathAddress, ModelNode> operations) throws XMLStreamException {
        ModelNode loader = Util.getEmptyOperation(ModelDescriptionConstants.ADD, null);
        String name = ModelKeys.LOADER_NAME;

        for (int i = 0; i < reader.getAttributeCount(); i++) {
            String value = reader.getAttributeValue(i);
            Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
            switch (attribute) {
                case CLASS: {
                    StoreConfigurationResource.CLASS.parseAndSetParameter(value, loader, reader);
                    break;
                }
                default: {
                    name = this.parseLoaderAttribute(name, reader, i, attribute, value, loader);
                }
            }
        }

        if (!loader.hasDefined(ModelKeys.CLASS)) {
            throw ParseUtils.missingRequired(reader, EnumSet.of(Attribute.CLASS));
        }

        // update the operation address with the name of this loader
        PathAddress loaderAddress = setOperationAddress(loader, PathAddress.pathAddress(cache.get(OP_ADDR)), LoaderConfigurationResource.LOADER_PATH, name);
        Map<PathAddress, ModelNode> additionalConfigurationOperations = new LinkedHashMap<>();
        this.parseLoaderElements(reader, loader, additionalConfigurationOperations);
        operations.put(loaderAddress, loader);
        operations.putAll(additionalConfigurationOperations);
    }

    private void parseClusterLoader(XMLExtendedStreamReader reader, ModelNode cache, Map<PathAddress, ModelNode> operations) throws XMLStreamException {
        // ModelNode for the cluster loader add operation
        ModelNode loader = Util.getEmptyOperation(ModelDescriptionConstants.ADD, null);
        String name = ModelKeys.CLUSTER_LOADER_NAME;

        for (int i = 0; i < reader.getAttributeCount(); i++) {
            String value = reader.getAttributeValue(i);
            Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
            switch (attribute) {
                case REMOTE_TIMEOUT: {
                    ClusterLoaderConfigurationResource.REMOTE_TIMEOUT.parseAndSetParameter(value, loader, reader);
                    break;
                }
                default: {
                    name = this.parseLoaderAttribute(name, reader, i, attribute, value, loader);
                }
            }
        }

        // update the cache address with the loader name
        PathAddress loaderAddress = setOperationAddress(loader, PathAddress.pathAddress(cache.get(OP_ADDR)), ClusterLoaderConfigurationResource.PATH, name);

        Map<PathAddress, ModelNode> additionalConfigurationOperations = new LinkedHashMap<>();
        this.parseLoaderElements(reader, loader, additionalConfigurationOperations);
        operations.put(loaderAddress, loader);
        operations.putAll(additionalConfigurationOperations);
    }

    private void parseCustomStore(XMLExtendedStreamReader reader, ModelNode cache, Map<PathAddress, ModelNode> operations) throws XMLStreamException {

       ModelNode store = Util.getEmptyOperation(ModelDescriptionConstants.ADD, null);
       String name = ModelKeys.STORE_NAME;

        for (int i = 0; i < reader.getAttributeCount(); i++) {
            String value = reader.getAttributeValue(i);
            Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
            switch (attribute) {
                case CLASS: {
                    StoreConfigurationResource.CLASS.parseAndSetParameter(value, store, reader);
                    break;
                }
                default: {
                    name = this.parseStoreAttribute(name, reader, i, attribute, value, store);
                }
            }
        }

        if (!store.hasDefined(ModelKeys.CLASS)) {
            throw ParseUtils.missingRequired(reader, EnumSet.of(Attribute.CLASS));
        }

        PathAddress storeAddress = setOperationAddress(store, PathAddress.pathAddress(cache.get(OP_ADDR)), StoreConfigurationResource.STORE_PATH, name);

        Map<PathAddress, ModelNode> additionalConfigurationOperations = new LinkedHashMap<>();
        this.parseStoreElements(reader, store, additionalConfigurationOperations);
        operations.put(storeAddress, store);
        operations.putAll(additionalConfigurationOperations);
    }

    private void parseFileStore(XMLExtendedStreamReader reader, ModelNode cache, Map<PathAddress, ModelNode> operations) throws XMLStreamException {


        ModelNode store = Util.getEmptyOperation(ModelDescriptionConstants.ADD, null);
        String name = ModelKeys.FILE_STORE_NAME;

        for (int i = 0; i < reader.getAttributeCount(); i++) {
            String value = reader.getAttributeValue(i);
            Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
            switch (attribute) {
                case MAX_ENTRIES: {
                    FileStoreResource.MAX_ENTRIES.parseAndSetParameter(value, store, reader);
                    break;
                }
                case RELATIVE_TO: {
                    FileStoreResource.RELATIVE_TO.parseAndSetParameter(value, store, reader);
                    break;
                }
                case PATH: {
                    FileStoreResource.PATH.parseAndSetParameter(value, store, reader);
                    break;
                }
                default: {
                    name = this.parseStoreAttribute(name, reader, i, attribute, value, store);
                }
            }
        }

        PathAddress storeAddress = setOperationAddress(store, PathAddress.pathAddress(cache.get(OP_ADDR)), FileStoreResource.FILE_STORE_PATH, name);

        Map<PathAddress, ModelNode> additionalConfigurationOperations = new LinkedHashMap<>();
        this.parseStoreElements(reader, store, additionalConfigurationOperations);
        operations.put(storeAddress, store);
        operations.putAll(additionalConfigurationOperations);
    }

    private void parseRemoteStore(XMLExtendedStreamReader reader, ModelNode cache, Map<PathAddress, ModelNode> operations) throws XMLStreamException {

       ModelNode store = Util.getEmptyOperation(ModelDescriptionConstants.ADD, null);
       String name = ModelKeys.REMOTE_STORE_NAME;

        Map<PathAddress, ModelNode> additionalConfigurationOperations = new LinkedHashMap<>();

        for (int i = 0; i < reader.getAttributeCount(); i++) {
            String value = reader.getAttributeValue(i);
            Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
            switch (attribute) {
                case CACHE: {
                    RemoteStoreConfigurationResource.CACHE.parseAndSetParameter(value, store, reader);
                    break;
                }
                case HOTROD_WRAPPING: {
                    RemoteStoreConfigurationResource.HOTROD_WRAPPING.parseAndSetParameter(value, store, reader);
                    break;
                }
                case RAW_VALUES: {
                    RemoteStoreConfigurationResource.RAW_VALUES.parseAndSetParameter(value, store, reader);
                    break;
                }
                case SOCKET_TIMEOUT: {
                    RemoteStoreConfigurationResource.SOCKET_TIMEOUT.parseAndSetParameter(value, store, reader);
                    break;
                }
                case TCP_NO_DELAY: {
                    RemoteStoreConfigurationResource.TCP_NO_DELAY.parseAndSetParameter(value, store, reader);
                    break;
                }

                default: {
                    name = this.parseStoreAttribute(name, reader, i, attribute, value, store);
                }
            }
        }

        PathAddress storeAddress = setOperationAddress(store, PathAddress.pathAddress(cache.get(OP_ADDR)), RemoteStoreConfigurationResource.REMOTE_STORE_PATH, name);

        while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
            Element element = Element.forName(reader.getLocalName());
            switch (element) {
                case REMOTE_SERVER: {
                    this.parseRemoteServer(reader, store.get(ModelKeys.REMOTE_SERVERS).add());
                    break;
                }
                case WRITE_BEHIND: {
                    parseStoreWriteBehind(reader, store, additionalConfigurationOperations);
                    break;
                }
                default: {
                    this.parseStoreProperty(reader, store, additionalConfigurationOperations);
                }
            }
        }

        if (!store.hasDefined(ModelKeys.REMOTE_SERVERS)) {
            throw ParseUtils.missingRequired(reader, Collections.singleton(Element.REMOTE_SERVER));
        }

        operations.put(storeAddress, store);
        operations.putAll(additionalConfigurationOperations);
    }

    private void parseLevelDBStore(XMLExtendedStreamReader reader, ModelNode cache, Map<PathAddress, ModelNode> operations) throws XMLStreamException {
        ModelNode store = Util.getEmptyOperation(ModelDescriptionConstants.ADD, null);
        String name = ModelKeys.LEVELDB_STORE_NAME;

        Map<PathAddress, ModelNode> additionalConfigurationOperations = new LinkedHashMap<>();

        for (int i = 0; i < reader.getAttributeCount(); i++) {
            String value = reader.getAttributeValue(i);
            Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
            switch (attribute) {
                case PATH: {
                    LevelDBStoreConfigurationResource.PATH.parseAndSetParameter(value, store, reader);
                    break;
                }
                case BLOCK_SIZE: {
                    LevelDBStoreConfigurationResource.BLOCK_SIZE.parseAndSetParameter(value, store, reader);
                    break;
                }
                case CACHE_SIZE: {
                    LevelDBStoreConfigurationResource.CACHE_SIZE.parseAndSetParameter(value, store, reader);
                    break;
                }
                case CLEAR_THRESHOLD: {
                    LevelDBStoreConfigurationResource.CLEAR_THRESHOLD.parseAndSetParameter(value, store, reader);
                    break;
                }
                default: {
                    name = this.parseStoreAttribute(name, reader, i, attribute, value, store);
                }
            }
        }

        PathAddress storeAddress = setOperationAddress(store, PathAddress.pathAddress(cache.get(OP_ADDR)), LevelDBStoreConfigurationResource.LEVELDB_STORE_PATH, name);

        while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
            Element element = Element.forName(reader.getLocalName());
            switch (element) {
                case EXPIRATION: {
                    this.parseStoreExpiry(reader, store, additionalConfigurationOperations);
                    break;
                }
                case COMPRESSION: {
                    this.parseStoreCompression(reader, store, additionalConfigurationOperations);
                    break;
                }
                case IMPLEMENTATION: {
                    this.parseStoreImplementation(reader, store, additionalConfigurationOperations);
                    break;
                }
                default: {
                    this.parseStoreProperty(reader, store, additionalConfigurationOperations);
                }
            }
        }

        operations.put(storeAddress, store);
        operations.putAll(additionalConfigurationOperations);
    }

    private void parseStoreExpiry(XMLExtendedStreamReader reader, ModelNode store, Map<PathAddress, ModelNode> operations) throws XMLStreamException {
        PathAddress storeExpiryAddress = PathAddress.pathAddress(store.get(OP_ADDR)).append(ModelKeys.EXPIRATION, ModelKeys.EXPIRATION_NAME);
        ModelNode storeExpiry = Util.createAddOperation(storeExpiryAddress);

        for (int i = 0; i < reader.getAttributeCount(); i++) {
            String value = reader.getAttributeValue(i);
            Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
            switch (attribute) {
                case PATH: {
                    LevelDBExpirationConfigurationResource.PATH.parseAndSetParameter(value, storeExpiry, reader);
                    break;
                }
                case QUEUE_SIZE: {
                    LevelDBExpirationConfigurationResource.QUEUE_SIZE.parseAndSetParameter(value, storeExpiry, reader);
                    break;
                }
                default:
                    throw ParseUtils.unexpectedAttribute(reader, i);
            }
        }
        ParseUtils.requireNoContent(reader);
        operations.put(storeExpiryAddress, storeExpiry);
    }

    private void parseStoreCompression(XMLExtendedStreamReader reader, ModelNode store, Map<PathAddress, ModelNode> operations) throws XMLStreamException {
        PathAddress storeCompressionAddress = PathAddress.pathAddress(store.get(OP_ADDR)).append(ModelKeys.COMPRESSION, ModelKeys.COMPRESSION_NAME);
        ModelNode storeCompression = Util.createAddOperation(storeCompressionAddress);

        for (int i = 0; i < reader.getAttributeCount(); i++) {
            String value = reader.getAttributeValue(i);
            Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
            switch (attribute) {
                case TYPE: {
                    LevelDBCompressionConfigurationResource.TYPE.parseAndSetParameter(value, storeCompression, reader);
                    break;
                }
                default:
                    throw ParseUtils.unexpectedAttribute(reader, i);
            }
        }
        ParseUtils.requireNoContent(reader);
        operations.put(storeCompressionAddress, storeCompression);
    }

    private void parseStoreImplementation(XMLExtendedStreamReader reader, ModelNode store, Map<PathAddress, ModelNode> operations) throws XMLStreamException {
        PathAddress storeImplementationAddress = PathAddress.pathAddress(store.get(OP_ADDR)).append(ModelKeys.IMPLEMENTATION, ModelKeys.IMPLEMENTATION_NAME);
        ModelNode storeImplementation = Util.createAddOperation(storeImplementationAddress);

        for (int i = 0; i < reader.getAttributeCount(); i++) {
            String value = reader.getAttributeValue(i);
            Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
            switch (attribute) {
                case TYPE: {
                    LevelDBImplementationConfigurationResource.TYPE.parseAndSetParameter(value, storeImplementation, reader);
                    break;
                }
                default:
                    throw ParseUtils.unexpectedAttribute(reader, i);
            }
        }
        ParseUtils.requireNoContent(reader);
        operations.put(storeImplementationAddress, storeImplementation);
    }

    private void parseRemoteServer(XMLExtendedStreamReader reader, ModelNode server) throws XMLStreamException {
        for (int i = 0; i < reader.getAttributeCount(); i++) {
            String value = reader.getAttributeValue(i);
            Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
            switch (attribute) {
                case OUTBOUND_SOCKET_BINDING: {
                    RemoteStoreConfigurationResource.OUTBOUND_SOCKET_BINDING.parseAndSetParameter(value, server, reader);
                    break;
                }
                default: {
                    throw ParseUtils.unexpectedAttribute(reader, i);
                }
            }
        }
        ParseUtils.requireNoContent(reader);
    }

    private void parseRestStore(XMLExtendedStreamReader reader, ModelNode cache, Map<PathAddress, ModelNode> operations) throws XMLStreamException {

        ModelNode store = Util.getEmptyOperation(ModelDescriptionConstants.ADD, null);
        String name = ModelKeys.REST_STORE_NAME;

         Map<PathAddress, ModelNode> additionalConfigurationOperations = new LinkedHashMap<>();

         for (int i = 0; i < reader.getAttributeCount(); i++) {
             String value = reader.getAttributeValue(i);
             Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
             switch (attribute) {
                 case APPEND_CACHE_NAME_TO_PATH: {
                     RestStoreConfigurationResource.APPEND_CACHE_NAME_TO_PATH.parseAndSetParameter(value, store, reader);
                     break;
                 }
                 case PATH: {
                     RestStoreConfigurationResource.PATH.parseAndSetParameter(value, store, reader);
                     break;
                 }

                 default: {
                     name = this.parseStoreAttribute(name, reader, i, attribute, value, store);
                 }
             }
         }

         PathAddress storeAddress = setOperationAddress(store, PathAddress.pathAddress(cache.get(OP_ADDR)), RestStoreConfigurationResource.REST_STORE_PATH, name);

         while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
             Element element = Element.forName(reader.getLocalName());
             switch (element) {
                 case CONNECTION_POOL: {
                     this.parseRestConnectionPool(reader, store.get(ModelKeys.CONNECTION_POOL).setEmptyObject());
                     break;
                 }
                 case REMOTE_SERVER: {
                     this.parseRemoteServer(reader, store.get(ModelKeys.REMOTE_SERVERS).add());
                     break;
                 }
                 case WRITE_BEHIND: {
                     parseStoreWriteBehind(reader, store, additionalConfigurationOperations);
                     break;
                 }
                 default: {
                     this.parseStoreProperty(reader, store, additionalConfigurationOperations);
                 }
             }
         }

         if (!store.hasDefined(ModelKeys.REMOTE_SERVERS)) {
             throw ParseUtils.missingRequired(reader, Collections.singleton(Element.REMOTE_SERVER));
         }

         operations.put(storeAddress, store);
         operations.putAll(additionalConfigurationOperations);
     }

    private void parseRestConnectionPool(XMLExtendedStreamReader reader, ModelNode table) throws XMLStreamException {
        for (int i = 0; i < reader.getAttributeCount(); i++) {
            String value = reader.getAttributeValue(i);
            Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
            switch (attribute) {
                case BUFFER_SIZE: {
                    RestStoreConfigurationResource.BUFFER_SIZE.parseAndSetParameter(value, table, reader);
                    break;
                }
                case CONNECTION_TIMEOUT: {
                    RestStoreConfigurationResource.CONNECTION_TIMEOUT.parseAndSetParameter(value, table, reader);
                    break;
                }
                case MAX_CONNECTIONS_PER_HOST: {
                    RestStoreConfigurationResource.MAX_CONNECTIONS_PER_HOST.parseAndSetParameter(value, table, reader);
                    break;
                }
                case MAX_TOTAL_CONNECTIONS: {
                    RestStoreConfigurationResource.MAX_TOTAL_CONNECTIONS.parseAndSetParameter(value, table, reader);
                    break;
                }
                case SOCKET_TIMEOUT: {
                    RestStoreConfigurationResource.SOCKET_TIMEOUT.parseAndSetParameter(value, table, reader);
                    break;
                }
                case TCP_NO_DELAY: {
                    RestStoreConfigurationResource.TCP_NO_DELAY.parseAndSetParameter(value, table, reader);
                    break;
                }
                default: {
                    throw ParseUtils.unexpectedAttribute(reader, i);
                }
            }
        }
        ParseUtils.requireNoContent(reader);
    }

    private void parseStringKeyedJDBCStore(XMLExtendedStreamReader reader, ModelNode cache, Map<PathAddress, ModelNode> operations) throws XMLStreamException {

       ModelNode store = Util.getEmptyOperation(ModelDescriptionConstants.ADD, null);
       String name = ModelKeys.STRING_KEYED_JDBC_STORE_NAME;

        Map<PathAddress, ModelNode> additionalConfigurationOperations = new LinkedHashMap<>();

        for (int i = 0; i < reader.getAttributeCount(); i++) {
            String value = reader.getAttributeValue(i);
            Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
            switch (attribute) {
                case DATASOURCE: {
                    BaseJDBCStoreConfigurationResource.DATA_SOURCE.parseAndSetParameter(value, store, reader);
                    break;
                }
                case DIALECT: {
                    BaseJDBCStoreConfigurationResource.DIALECT.parseAndSetParameter(value, store, reader);
                    break;
                }
                default: {
                    name = this.parseStoreAttribute(name, reader, i, attribute, value, store);
                }
            }
        }

        if (!store.hasDefined(ModelKeys.DATASOURCE)) {
            throw ParseUtils.missingRequired(reader, EnumSet.of(Attribute.DATASOURCE));
        }

        PathAddress storeAddress = setOperationAddress(store, PathAddress.pathAddress(cache.get(OP_ADDR)), StringKeyedJDBCStoreResource.PATH, name);

        while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
            Element element = Element.forName(reader.getLocalName());
            switch (element) {
                case STRING_KEYED_TABLE: {
                    this.parseJDBCStoreTable(reader, store.get(ModelKeys.STRING_KEYED_TABLE).setEmptyObject());
                    break;
                }
                case WRITE_BEHIND: {
                    parseStoreWriteBehind(reader, store, additionalConfigurationOperations);
                    break;
                }
                default: {
                    this.parseStoreProperty(reader, store, additionalConfigurationOperations);
                }
            }
        }

        operations.put(storeAddress, store);
        operations.putAll(additionalConfigurationOperations);
    }

    private void parseBinaryKeyedJDBCStore(XMLExtendedStreamReader reader, ModelNode cache, Map<PathAddress, ModelNode> operations) throws XMLStreamException {

        ModelNode store = Util.getEmptyOperation(ModelDescriptionConstants.ADD, null);
        String name = ModelKeys.BINARY_KEYED_JDBC_STORE_NAME;

        Map<PathAddress, ModelNode> additionalConfigurationOperations = new LinkedHashMap<>();

        for (int i = 0; i < reader.getAttributeCount(); i++) {
            String value = reader.getAttributeValue(i);
            Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
            switch (attribute) {
                case DATASOURCE: {
                    BaseJDBCStoreConfigurationResource.DATA_SOURCE.parseAndSetParameter(value, store, reader);
                    break;
                }
                case DIALECT: {
                    BaseJDBCStoreConfigurationResource.DIALECT.parseAndSetParameter(value, store, reader);
                    break;
                }
                default: {
                    name = this.parseStoreAttribute(name, reader, i, attribute, value, store);
                }
            }
        }

        if (!store.hasDefined(ModelKeys.DATASOURCE)) {
            throw ParseUtils.missingRequired(reader, EnumSet.of(Attribute.DATASOURCE));
        }

        PathAddress storeAddress = setOperationAddress(store, PathAddress.pathAddress(cache.get(OP_ADDR)), BinaryKeyedJDBCStoreConfigurationResource.PATH, name);

        while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
            Element element = Element.forName(reader.getLocalName());
            switch (element) {
                case BINARY_KEYED_TABLE: {
                    this.parseJDBCStoreTable(reader, store.get(ModelKeys.BINARY_KEYED_TABLE).setEmptyObject());
                    break;
                }
                case WRITE_BEHIND: {
                    parseStoreWriteBehind(reader, store, additionalConfigurationOperations);
                    break;
                }
                default: {
                    this.parseStoreProperty(reader, store, additionalConfigurationOperations);
                }
            }
        }

        operations.put(storeAddress, store);
        operations.putAll(additionalConfigurationOperations);
    }
    private void parseMixedKeyedJDBCStore(XMLExtendedStreamReader reader, ModelNode cache, Map<PathAddress, ModelNode> operations) throws XMLStreamException {

        ModelNode store = Util.getEmptyOperation(ModelDescriptionConstants.ADD, null);
        String name = ModelKeys.MIXED_KEYED_JDBC_STORE_NAME;

        Map<PathAddress, ModelNode> additionalConfigurationOperations = new LinkedHashMap<>();

        for (int i = 0; i < reader.getAttributeCount(); i++) {
            String value = reader.getAttributeValue(i);
            Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
            switch (attribute) {
                case DATASOURCE: {
                    BaseJDBCStoreConfigurationResource.DATA_SOURCE.parseAndSetParameter(value, store, reader);
                    break;
                }
                case DIALECT: {
                    BaseJDBCStoreConfigurationResource.DIALECT.parseAndSetParameter(value, store, reader);
                    break;
                }
                default: {
                    name = this.parseStoreAttribute(name, reader, i, attribute, value, store);
                }
            }
        }

        if (!store.hasDefined(ModelKeys.DATASOURCE)) {
            throw ParseUtils.missingRequired(reader, EnumSet.of(Attribute.DATASOURCE));
        }

        PathAddress storeAddress = setOperationAddress(store, PathAddress.pathAddress(cache.get(OP_ADDR)), MixedKeyedJDBCStoreConfigurationResource.PATH, name);

        while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
            Element element = Element.forName(reader.getLocalName());
            switch (element) {
                case STRING_KEYED_TABLE: {
                    this.parseJDBCStoreTable(reader, store.get(ModelKeys.STRING_KEYED_TABLE).setEmptyObject());
                    break;
                }
                case BINARY_KEYED_TABLE: {
                    this.parseJDBCStoreTable(reader, store.get(ModelKeys.BINARY_KEYED_TABLE).setEmptyObject());
                    break;
                }
                case WRITE_BEHIND: {
                    parseStoreWriteBehind(reader, store, additionalConfigurationOperations);
                    break;
                }
                case PROPERTY: {
                    parseStoreProperty(reader, store, additionalConfigurationOperations);
                    break;
                }
                default:
                    throw ParseUtils.unexpectedElement(reader);
            }
        }

        operations.put(storeAddress, store);
        operations.putAll(additionalConfigurationOperations);
    }

    private void parseJDBCStoreTable(XMLExtendedStreamReader reader, ModelNode table) throws XMLStreamException {
        for (int i = 0; i < reader.getAttributeCount(); i++) {
            String value = reader.getAttributeValue(i);
            Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
            switch (attribute) {
                case PREFIX: {
                    BaseJDBCStoreConfigurationResource.PREFIX.parseAndSetParameter(value, table, reader);
                    break;
                }
                case FETCH_SIZE: {
                    BaseJDBCStoreConfigurationResource.FETCH_SIZE.parseAndSetParameter(value, table, reader);
                    break;
                }
                case BATCH_SIZE: {
                    BaseJDBCStoreConfigurationResource.BATCH_SIZE.parseAndSetParameter(value, table, reader);
                    break;
                }
                case CREATE_ON_START: {
                    if (namespace.since(Namespace.INFINISPAN_SERVER_7_2)) {
                        BaseJDBCStoreConfigurationResource.CREATE_ON_START.parseAndSetParameter(value, table, reader);
                        break;
                    }
                }
                case DROP_ON_EXIT: {
                    if (namespace.since(Namespace.INFINISPAN_SERVER_7_2)) {
                        BaseJDBCStoreConfigurationResource.DROP_ON_EXIT.parseAndSetParameter(value, table, reader);
                        break;
                    }
                }
                default: {
                    throw ParseUtils.unexpectedAttribute(reader, i);
                }
            }
        }

        while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
            Element element = Element.forName(reader.getLocalName());
            switch (element) {
                case ID_COLUMN: {
                    this.parseJDBCStoreColumn(reader, table.get(ModelKeys.ID_COLUMN).setEmptyObject());
                    break;
                }
                case DATA_COLUMN: {
                    this.parseJDBCStoreColumn(reader, table.get(ModelKeys.DATA_COLUMN).setEmptyObject());
                    break;
                }
                case TIMESTAMP_COLUMN: {
                    this.parseJDBCStoreColumn(reader, table.get(ModelKeys.TIMESTAMP_COLUMN).setEmptyObject());
                    break;
                }
                default: {
                    throw ParseUtils.unexpectedElement(reader);
                }
            }
        }
    }

    private void parseJDBCStoreColumn(XMLExtendedStreamReader reader, ModelNode column) throws XMLStreamException {
        for (int i = 0; i < reader.getAttributeCount(); i++) {
            String value = reader.getAttributeValue(i);
            Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
            switch (attribute) {
                case NAME: {
                    BaseJDBCStoreConfigurationResource.COLUMN_NAME.parseAndSetParameter(value, column, reader);
                    break;
                }
                case TYPE: {
                    BaseJDBCStoreConfigurationResource.COLUMN_TYPE.parseAndSetParameter(value, column, reader);
                    break;
                }
                default: {
                    throw ParseUtils.unexpectedAttribute(reader, i);
                }
            }
        }
        ParseUtils.requireNoContent(reader);
    }

    private void parseLoaderElements(XMLExtendedStreamReader reader, ModelNode loader, Map<PathAddress, ModelNode> operations) throws XMLStreamException {
        while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
            Element element = Element.forName(reader.getLocalName());
            switch (element) {
                case PROPERTY: {
                    parseStoreProperty(reader, loader, operations);
                    break;
                }
                default:
                    throw ParseUtils.unexpectedElement(reader);
            }
        }
    }

    private String parseLoaderAttribute(String name, XMLExtendedStreamReader reader, int index, Attribute attribute, String value, ModelNode loader) throws XMLStreamException {
        switch (attribute) {
            case NAME: {
                name = value;
                BaseLoaderConfigurationResource.NAME.parseAndSetParameter(value, loader, reader);
                break;
            }
            case SHARED: {
                BaseLoaderConfigurationResource.SHARED.parseAndSetParameter(value, loader, reader);
                break;
            }
            case PRELOAD: {
                BaseLoaderConfigurationResource.PRELOAD.parseAndSetParameter(value, loader, reader);
                break;
            }
            default: {
                throw ParseUtils.unexpectedAttribute(reader, index);
            }
        }
        return name;
    }

    private String parseStoreAttribute(String name, XMLExtendedStreamReader reader, int index, Attribute attribute, String value, ModelNode store) throws XMLStreamException {
        switch (attribute) {
            case NAME: {
                name = value;
                BaseStoreConfigurationResource.NAME.parseAndSetParameter(value, store, reader);
                break;
            }
            case SHARED: {
                BaseStoreConfigurationResource.SHARED.parseAndSetParameter(value, store, reader);
                break;
            }
            case PRELOAD: {
                BaseStoreConfigurationResource.PRELOAD.parseAndSetParameter(value, store, reader);
                break;
            }
            case PASSIVATION: {
                BaseStoreConfigurationResource.PASSIVATION.parseAndSetParameter(value, store, reader);
                break;
            }
            case FETCH_STATE: {
                BaseStoreConfigurationResource.FETCH_STATE.parseAndSetParameter(value, store, reader);
                break;
            }
            case PURGE: {
                BaseStoreConfigurationResource.PURGE.parseAndSetParameter(value, store, reader);
                break;
            }
            case SINGLETON: {
                BaseStoreConfigurationResource.SINGLETON.parseAndSetParameter(value, store, reader);
                break;
            }
            case READ_ONLY: {
                BaseStoreConfigurationResource.READ_ONLY.parseAndSetParameter(value, store, reader);
                break;
            }
            default: {
                throw ParseUtils.unexpectedAttribute(reader, index);
            }
        }
        return name;
    }

    private void parseStoreElements(XMLExtendedStreamReader reader, ModelNode store, Map<PathAddress, ModelNode> operations) throws XMLStreamException {
        while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
            Element element = Element.forName(reader.getLocalName());
            switch (element) {
                case WRITE_BEHIND: {
                    parseStoreWriteBehind(reader, store, operations);
                    break;
                }
                case PROPERTY: {
                    parseStoreProperty(reader, store, operations);
                    break;
                }
                default:
                    throw ParseUtils.unexpectedElement(reader);
            }
        }
    }

    private void parseStoreWriteBehind(XMLExtendedStreamReader reader, ModelNode store, Map<PathAddress, ModelNode> operations) throws XMLStreamException {

        PathAddress writeBehindAddress = PathAddress.pathAddress(store.get(OP_ADDR)).append(ModelKeys.WRITE_BEHIND, ModelKeys.WRITE_BEHIND_NAME);
        ModelNode writeBehind = Util.createAddOperation(writeBehindAddress);

        for (int i = 0; i < reader.getAttributeCount(); i++) {
            String value = reader.getAttributeValue(i);
            Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
            switch (attribute) {
                case FLUSH_LOCK_TIMEOUT: {
                    StoreWriteBehindResource.FLUSH_LOCK_TIMEOUT.parseAndSetParameter(value, writeBehind, reader);
                    break;
                }
                case MODIFICATION_QUEUE_SIZE: {
                    StoreWriteBehindResource.MODIFICATION_QUEUE_SIZE.parseAndSetParameter(value, writeBehind, reader);
                    break;
                }
                case SHUTDOWN_TIMEOUT: {
                    StoreWriteBehindResource.SHUTDOWN_TIMEOUT.parseAndSetParameter(value, writeBehind, reader);
                    break;
                }
                case THREAD_POOL_SIZE: {
                    StoreWriteBehindResource.THREAD_POOL_SIZE.parseAndSetParameter(value, writeBehind, reader);
                    break;
                }
                default:
                    throw ParseUtils.unexpectedAttribute(reader, i);
            }
        }
        ParseUtils.requireNoContent(reader);
        operations.put(writeBehindAddress, writeBehind);
    }

    private void parseStoreProperty(XMLExtendedStreamReader reader, ModelNode node, final Map<PathAddress, ModelNode> operations) throws XMLStreamException {

        int attributes = reader.getAttributeCount();
        String propertyName = null;
        for (int i = 0; i < attributes; i++) {
            String value = reader.getAttributeValue(i);
            Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
            switch (attribute) {
                case NAME: {
                    propertyName = value;
                    break;
                }
                default: {
                    throw ParseUtils.unexpectedAttribute(reader, i);
                }
            }
        }
        if (propertyName == null) {
            throw ParseUtils.missingRequired(reader, Collections.singleton(Attribute.NAME));
        }
        String propertyValue = reader.getElementText();

        PathAddress propertyAddress = PathAddress.pathAddress(node.get(OP_ADDR)).append(ModelKeys.PROPERTY, propertyName);
        ModelNode property = Util.createAddOperation(propertyAddress);

        // represent the value as a ModelNode to cater for expressions
        StorePropertyResource.VALUE.parseAndSetParameter(propertyValue, property, reader);

        operations.put(propertyAddress, property);
    }

    private void parseIndexing(XMLExtendedStreamReader reader, ModelNode node) throws XMLStreamException {
        for (int i = 0; i < reader.getAttributeCount(); i++) {
            String value = reader.getAttributeValue(i);
            Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
            switch (attribute) {
                case INDEX:
                    CacheConfigurationResource.INDEXING.parseAndSetParameter(value, node, reader);
                    break;
                case AUTO_CONFIG:
                    CacheConfigurationResource.INDEXING_AUTO_CONFIG.parseAndSetParameter(value, node, reader);
                    break;
                default:
                    throw ParseUtils.unexpectedAttribute(reader, i);
            }
        }

        while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
            Element element = Element.forName(reader.getLocalName());
            switch (element) {
                case PROPERTY: {
                    int attributes = reader.getAttributeCount();
                    String property = null;
                    for (int i = 0; i < attributes; i++) {
                        String value = reader.getAttributeValue(i);
                        Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
                        switch (attribute) {
                            case NAME: {
                                property = value;
                                break;
                            }
                            default: {
                                throw ParseUtils.unexpectedAttribute(reader, i);
                            }
                        }
                    }
                    if (property == null) {
                        throw ParseUtils.missingRequired(reader, Collections.singleton(Attribute.NAME));
                    }
                    String value = reader.getElementText();
                    CacheConfigurationResource.INDEXING_PROPERTIES.parseAndAddParameterElement(property, value, node, reader);
                    break;
                }
                default: {
                    throw ParseUtils.unexpectedElement(reader);
                }
            }
        }
       // ParseUtils.requireNoContent(reader);
    }

    private void parseBackups(XMLExtendedStreamReader reader, ModelNode cache, Map<PathAddress, ModelNode> operations) throws XMLStreamException {

        while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
            Element element = Element.forName(reader.getLocalName());
            switch (element) {
                case BACKUP: {
                    this.parseBackup(reader, cache, operations);
                    break;
                }
                default: {
                    throw ParseUtils.unexpectedElement(reader);
                }
            }
        }
    }

   private void parseBackupFor(XMLExtendedStreamReader reader, ModelNode cache) throws XMLStreamException {

      for (int i = 0; i < reader.getAttributeCount(); i++) {
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case REMOTE_CACHE: {
                CacheConfigurationResource.REMOTE_CACHE.parseAndSetParameter(value, cache, reader);
               break;
            }
            case REMOTE_SITE: {
                CacheConfigurationResource.REMOTE_SITE.parseAndSetParameter(value, cache, reader);
               break;
            }
            default: {
               throw ParseUtils.unexpectedAttribute(reader, i);
            }
         }
      }
      ParseUtils.requireNoContent(reader);
   }

    private void parseBackup(XMLExtendedStreamReader reader, ModelNode cache, Map<PathAddress, ModelNode> operations) throws XMLStreamException {

        ModelNode operation = Util.createAddOperation();
        String site = null;

        for (int i = 0; i < reader.getAttributeCount(); i++) {
            String value = reader.getAttributeValue(i);
            Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
            switch (attribute) {
                case SITE: {
                    site = value;
                    break;
                }
                case STRATEGY: {
                    BackupSiteConfigurationResource.STRATEGY.parseAndSetParameter(value, operation, reader);
                    break;
                }
                case BACKUP_FAILURE_POLICY: {
                    BackupSiteConfigurationResource.FAILURE_POLICY.parseAndSetParameter(value, operation, reader);
                    break;
                }
                case TIMEOUT: {
                    BackupSiteConfigurationResource.REPLICATION_TIMEOUT.parseAndSetParameter(value, operation, reader);
                    break;
                }
                case ENABLED: {
                    BackupSiteConfigurationResource.ENABLED.parseAndSetParameter(value, operation, reader);
                    break;
                }
                default: {
                    throw ParseUtils.unexpectedAttribute(reader, i);
                }
            }
        }

        if (site == null) {
            throw ParseUtils.missingRequired(reader, Collections.singleton(Attribute.SITE));
        }

        PathAddress address = PathAddress.pathAddress(cache.get(OP_ADDR)).append(ModelKeys.BACKUP, site);
        operation.get(OP_ADDR).set(address.toModelNode());
        Map<PathAddress, ModelNode> additionalOperations = new LinkedHashMap<>(1);

        while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
            Element element = Element.forName(reader.getLocalName());
            switch (element) {
                case TAKE_OFFLINE: {
                    this.parseTakeOffline(reader, operation);
                    break;
                }
                case STATE_TRANSFER:
                   if (namespace.since(Namespace.INFINISPAN_SERVER_7_0)) {
                       this.parseXSiteStateTransfer(reader, operation, additionalOperations);
                       break;
                   }
                default: {
                    throw ParseUtils.unexpectedElement(reader);
                }
            }
        }

        operations.put(address, operation);
        operations.putAll(additionalOperations);
    }

    private void parseTakeOffline(XMLExtendedStreamReader reader, ModelNode operation) throws XMLStreamException {
        for (int i = 0; i < reader.getAttributeCount(); i++) {
            String value = reader.getAttributeValue(i);
            Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
            switch (attribute) {
                case TAKE_BACKUP_OFFLINE_AFTER_FAILURES: {
                    BackupSiteConfigurationResource.TAKE_OFFLINE_AFTER_FAILURES.parseAndSetParameter(value, operation, reader);
                    break;
                }
                case TAKE_BACKUP_OFFLINE_MIN_WAIT: {
                    BackupSiteConfigurationResource.TAKE_OFFLINE_MIN_WAIT.parseAndSetParameter(value, operation, reader);
                    break;
                }
                default: {
                    throw ParseUtils.unexpectedAttribute(reader, i);
                }
            }
        }
        ParseUtils.requireNoContent(reader);
    }

    private void parseXSiteStateTransfer(XMLExtendedStreamReader reader, ModelNode backup, Map<PathAddress, ModelNode> operations) throws XMLStreamException {
       PathAddress address = PathAddress.pathAddress(backup.get(OP_ADDR)).append(ModelKeys.STATE_TRANSFER, ModelKeys.STATE_TRANSFER_NAME);
       ModelNode operation = Util.createAddOperation(address);

       for (int i = 0; i < reader.getAttributeCount(); i++) {
            String value = reader.getAttributeValue(i);
            Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
            switch (attribute) {
                case CHUNK_SIZE:
                    BackupSiteStateTransferConfigurationResource.STATE_TRANSFER_CHUNK_SIZE.parseAndSetParameter(value, operation, reader);
                    break;
                case TIMEOUT:
                    BackupSiteStateTransferConfigurationResource.STATE_TRANSFER_TIMEOUT.parseAndSetParameter(value, operation, reader);
                    break;
                case MAX_RETRIES:
                    BackupSiteStateTransferConfigurationResource.STATE_TRANSFER_MAX_RETRIES.parseAndSetParameter(value, operation, reader);
                    break;
                case WAIT_TIME:
                    BackupSiteStateTransferConfigurationResource.STATE_TRANSFER_WAIT_TIME.parseAndSetParameter(value, operation, reader);
                    break;
                default:
                    throw ParseUtils.unexpectedAttribute(reader, i);
            }
       }
       ParseUtils.requireNoContent(reader);
       operations.put(address, operation);
   }

    private void parseCompatibility(XMLExtendedStreamReader reader, ModelNode cache, Map<PathAddress, ModelNode> operations) throws XMLStreamException {
        PathAddress compatibilityAddress = PathAddress.pathAddress(cache.get(OP_ADDR)).append(ModelKeys.COMPATIBILITY, ModelKeys.COMPATIBILITY_NAME);
        ModelNode compatibility = Util.createAddOperation(compatibilityAddress);

        for (int i = 0; i < reader.getAttributeCount(); i++) {
            String value = reader.getAttributeValue(i);
            Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
            switch (attribute) {
                case ENABLED: {
                    CompatibilityConfigurationResource.ENABLED.parseAndSetParameter(value, compatibility, reader);
                    break;
                }
                case MARSHALLER: {
                    CompatibilityConfigurationResource.MARSHALLER.parseAndSetParameter(value, compatibility, reader);
                    break;
                }
                default: {
                    throw ParseUtils.unexpectedAttribute(reader, i);
                }
            }
        }
        ParseUtils.requireNoContent(reader);
        operations.put(compatibilityAddress, compatibility);
    }

    private void parseCacheSecurity(XMLExtendedStreamReader reader, ModelNode cache, Map<PathAddress, ModelNode> operations) throws XMLStreamException {
        PathAddress securityAddress = PathAddress.pathAddress(cache.get(OP_ADDR)).append(ModelKeys.SECURITY, ModelKeys.SECURITY_NAME);
        ModelNode security = Util.createAddOperation(securityAddress);

        ParseUtils.requireNoAttributes(reader);
        Map<PathAddress, ModelNode> additionalConfigurationOperations = new LinkedHashMap<>();

        while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
            Element element = Element.forName(reader.getLocalName());
            switch (element) {
                case AUTHORIZATION: {
                    this.parseCacheAuthorization(reader, security, additionalConfigurationOperations);
                    break;
                }
                default: {
                    throw ParseUtils.unexpectedElement(reader);
                }
            }
        }
        operations.put(securityAddress, security);
        operations.putAll(additionalConfigurationOperations);
    }

    private void parseCacheAuthorization(XMLExtendedStreamReader reader, ModelNode security, Map<PathAddress, ModelNode> operations) throws XMLStreamException {
        PathAddress authorizationAddress = PathAddress.pathAddress(security.get(OP_ADDR)).append(ModelKeys.AUTHORIZATION, ModelKeys.AUTHORIZATION_NAME);
        ModelNode authorization = Util.createAddOperation(authorizationAddress);

        for (int i = 0; i < reader.getAttributeCount(); i++) {
            String value = reader.getAttributeValue(i);
            Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
            switch (attribute) {
                case ENABLED: {
                    CacheAuthorizationConfigurationResource.ENABLED.parseAndSetParameter(value, authorization, reader);
                    break;
                }
                case ROLES: {
                    for(String role : reader.getListAttributeValue(i)) {
                        CacheAuthorizationConfigurationResource.ROLES.parseAndAddParameterElement(role, authorization, reader);
                    }
                    break;
                }
                default: {
                    throw ParseUtils.unexpectedAttribute(reader, i);
                }
            }
        }

        ParseUtils.requireNoContent(reader);
        operations.put(authorizationAddress, authorization);
    }

   private void parsePartitionHandling(XMLExtendedStreamReader reader, ModelNode cache, Map<PathAddress, ModelNode> operations) throws XMLStreamException {

      PathAddress partitionHandlingAddress = PathAddress.pathAddress(cache.get(OP_ADDR)).append(ModelKeys.PARTITION_HANDLING, ModelKeys.PARTITION_HANDLING_NAME);
      ModelNode partitionHandling = Util.createAddOperation(partitionHandlingAddress);

      for (int i = 0; i < reader.getAttributeCount(); i++) {
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case ENABLED: {
               PartitionHandlingConfigurationResource.ENABLED.parseAndSetParameter(value, partitionHandling, reader);
               break;
            }
            default: {
               throw ParseUtils.unexpectedAttribute(reader, i);
            }
         }
      }
      ParseUtils.requireNoContent(reader);
      operations.put(partitionHandlingAddress, partitionHandling);
   }

   private void parseThreadPool(ThreadPoolResource pool, XMLExtendedStreamReader reader, PathAddress parentAddress, Map<PathAddress, ModelNode> operations) throws XMLStreamException {
       PathAddress address = parentAddress.append(pool.getPathElement());
       ModelNode operation = Util.createAddOperation(address);
       operations.put(address, operation);

       for (int i = 0; i < reader.getAttributeCount(); i++) {
           Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
           switch (attribute) {
               case MIN_THREADS: {
                   readAttribute(reader, i, operation, pool.getMinThreads());
                   break;
               }
               case MAX_THREADS: {
                   readAttribute(reader, i, operation, pool.getMaxThreads());
                   break;
               }
               case QUEUE_LENGTH: {
                   readAttribute(reader, i, operation, pool.getQueueLength());
                   break;
               }
               case KEEPALIVE_TIME: {
                   readAttribute(reader, i, operation, pool.getKeepAliveTime());
                   break;
               }
               default: {
                   throw ParseUtils.unexpectedAttribute(reader, i);
               }
           }
       }

       ParseUtils.requireNoContent(reader);
   }

   private void parseScheduledThreadPool(ScheduledThreadPoolResource pool, XMLExtendedStreamReader reader, PathAddress parentAddress, Map<PathAddress, ModelNode> operations) throws XMLStreamException {
       PathAddress address = parentAddress.append(pool.getPathElement());
       ModelNode operation = Util.createAddOperation(address);
       operations.put(address, operation);

       for (int i = 0; i < reader.getAttributeCount(); i++) {
           Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
           switch (attribute) {
               case MAX_THREADS: {
                   readAttribute(reader, i, operation, pool.getMaxThreads());
                   break;
               }
               case KEEPALIVE_TIME: {
                   readAttribute(reader, i, operation, pool.getKeepAliveTime());
                   break;
               }
               default: {
                   throw ParseUtils.unexpectedAttribute(reader, i);
               }
           }
       }

       ParseUtils.requireNoContent(reader);
   }

   private static void readAttribute(XMLExtendedStreamReader reader, int index, ModelNode operation, AttributeDefinition attribute) throws XMLStreamException {
       setAttribute(reader, reader.getAttributeValue(index), operation, attribute);
   }

   private static void setAttribute(XMLExtendedStreamReader reader, String value, ModelNode operation, AttributeDefinition attribute) throws XMLStreamException {
       attribute.getParser().parseAndSetParameter(attribute, value, operation, reader);
   }
}
