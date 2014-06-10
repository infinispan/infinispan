package org.jboss.as.clustering.infinispan.subsystem;

import org.infinispan.security.impl.ClusterRoleMapper;
import org.infinispan.security.impl.CommonNameRoleMapper;
import org.infinispan.security.impl.IdentityRoleMapper;
import org.jboss.as.controller.PathAddress;
import org.jboss.as.controller.descriptions.ModelDescriptionConstants;
import org.jboss.as.controller.operations.common.Util;
import org.jboss.as.controller.parsing.ParseUtils;
import org.jboss.dmr.ModelNode;
import org.jboss.staxmapper.XMLElementReader;
import org.jboss.staxmapper.XMLExtendedStreamReader;

import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;

import java.util.*;

import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.ADD;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.OP_ADDR;

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
public final class InfinispanSubsystemXMLReader_7_0 implements XMLElementReader<List<ModelNode>> {

    /**
     * {@inheritDoc}
     * @see org.jboss.staxmapper.XMLElementReader#readElement(org.jboss.staxmapper.XMLExtendedStreamReader, Object)
     */
    @Override
    public void readElement(XMLExtendedStreamReader reader, List<ModelNode> operations) throws XMLStreamException {

        PathAddress subsystemAddress = PathAddress.pathAddress(InfinispanExtension.SUBSYSTEM_PATH);
        ModelNode subsystem = Util.createAddOperation(subsystemAddress);

        // command to add the subsystem
        operations.add(subsystem);

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
    }

    private void parseContainer(XMLExtendedStreamReader reader, PathAddress subsystemAddress, List<ModelNode> operations) throws XMLStreamException {

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
                    CacheContainerResource.LISTENER_EXECUTOR.parseAndSetParameter(value, container, reader);
                    break;
                }
                case EVICTION_EXECUTOR: {
                    CacheContainerResource.EVICTION_EXECUTOR.parseAndSetParameter(value, container, reader);
                    break;
                }
                case REPLICATION_QUEUE_EXECUTOR: {
                    CacheContainerResource.REPLICATION_QUEUE_EXECUTOR.parseAndSetParameter(value, container, reader);
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

        // operation to add the container
        operations.add(container);

        while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
            Element element = Element.forName(reader.getLocalName());
            switch (element) {
                case TRANSPORT: {
                    parseTransport(reader, containerAddress, operations);
                    break;
                }
                case SECURITY: {
                    parseGlobalSecurity(reader, containerAddress, operations);
                    break;
                }
                case LOCAL_CACHE: {
                    parseLocalCache(reader, containerAddress, operations);
                    break;
                }
                case INVALIDATION_CACHE: {
                    parseInvalidationCache(reader, containerAddress, operations);
                    break;
                }
                case REPLICATED_CACHE: {
                    parseReplicatedCache(reader, containerAddress, operations);
                    break;
                }
                case DISTRIBUTED_CACHE: {
                    parseDistributedCache(reader, containerAddress, operations);
                    break;
                }
                default: {
                    throw ParseUtils.unexpectedElement(reader);
                }
            }
        }
    }

   private void parseTransport(XMLExtendedStreamReader reader, PathAddress containerAddress, List<ModelNode> operations) throws XMLStreamException {

        PathAddress transportAddress = containerAddress.append(ModelKeys.TRANSPORT, ModelKeys.TRANSPORT_NAME);
        ModelNode transport = Util.createAddOperation(transportAddress);

        for (int i = 0; i < reader.getAttributeCount(); i++) {
            String value = reader.getAttributeValue(i);
            Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
            switch (attribute) {
                case STACK: {
                    TransportResource.STACK.parseAndSetParameter(value, transport, reader);
                    break;
                }
                case CLUSTER: {
                    TransportResource.CLUSTER.parseAndSetParameter(value, transport, reader);
                    break;
                }
                case EXECUTOR: {
                    TransportResource.EXECUTOR.parseAndSetParameter(value, transport, reader);
                    break;
                }
                case LOCK_TIMEOUT: {
                    TransportResource.LOCK_TIMEOUT.parseAndSetParameter(value, transport, reader);
                    break;
                }
                case STRICT_PEER_TO_PEER: {
                    TransportResource.STRICT_PEER_TO_PEER.parseAndSetParameter(value, transport, reader);
                    break;
                }
                default: {
                    throw ParseUtils.unexpectedAttribute(reader, i);
                }
            }
        }
        ParseUtils.requireNoContent(reader);

        operations.add(transport);
    }

    private void parseGlobalSecurity(XMLExtendedStreamReader reader, PathAddress containerAddress, List<ModelNode> operations) throws XMLStreamException {
        PathAddress securityAddress = containerAddress.append(ModelKeys.SECURITY, ModelKeys.SECURITY_NAME);
        ModelNode security = Util.createAddOperation(securityAddress);

        ParseUtils.requireNoAttributes(reader);

        List<ModelNode> additionalConfigurationOperations = new ArrayList<ModelNode>();

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

        operations.add(security);
        // add operations to create configuration resources
        for (ModelNode additionalOperation : additionalConfigurationOperations) {
            operations.add(additionalOperation);
        }
    }

    private void parseGlobalAuthorization(XMLExtendedStreamReader reader, PathAddress securityAddress, List<ModelNode> operations) throws XMLStreamException {
        PathAddress authorizationAddress = securityAddress.append(ModelKeys.AUTHORIZATION, ModelKeys.AUTHORIZATION_NAME);
        ModelNode authorization = Util.createAddOperation(authorizationAddress);

        for (int i = 0; i < reader.getAttributeCount(); i++) {
            String value = reader.getAttributeValue(i);
            Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
            switch (attribute) {
                case AUDIT_LOGGER: {
                    // Ignore
                    break;
                }
                default: {
                    throw ParseUtils.unexpectedAttribute(reader, i);
                }
            }
        }

        List<ModelNode> additionalConfigurationOperations = new ArrayList<ModelNode>();
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

        operations.add(authorization);
        // add operations to create configuration resources
        for (ModelNode additionalOperation : additionalConfigurationOperations) {
            operations.add(additionalOperation);
        }
    }

    private void parseGlobalRole(XMLExtendedStreamReader reader, PathAddress authorizationAddress, List<ModelNode> operations) throws XMLStreamException {
        String[] attributes = ParseUtils.requireAttributes(reader, Attribute.NAME.getLocalName(), Attribute.PERMISSIONS.getLocalName());
        String name = attributes[0];
        PathAddress roleAddress = authorizationAddress.append(ModelKeys.ROLE, name);
        ModelNode role = Util.createAddOperation(roleAddress);
        AuthorizationRoleResource.NAME.parseAndSetParameter(name, role, reader);
        for(String perm : attributes[1].split("\\s+")) {
            AuthorizationRoleResource.PERMISSIONS.parseAndAddParameterElement(perm, role, reader);
        }

        ParseUtils.requireNoContent(reader);
        operations.add(role);
    }

    private void parseCacheAttribute(XMLExtendedStreamReader reader, int index, Attribute attribute, String value, ModelNode cache) throws XMLStreamException {
        switch (attribute) {
            case NAME: {
                CacheResource.NAME.parseAndSetParameter(value, cache, reader);
                break;
            }
            case START: {
                CacheResource.START.parseAndSetParameter(value, cache, reader);
                break;
            }
            case JNDI_NAME: {
                CacheResource.JNDI_NAME.parseAndSetParameter(value, cache, reader);
                break;
            }
            case BATCHING: {
                CacheResource.BATCHING.parseAndSetParameter(value, cache, reader);
                break;
            }
            case MODULE: {
                CacheResource.CACHE_MODULE.parseAndSetParameter(value, cache, reader);
                break;
            }
            case STATISTICS: {
                CacheResource.STATISTICS.parseAndSetParameter(value, cache, reader);
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
                ClusteredCacheResource.ASYNC_MARSHALLING.parseAndSetParameter(value, cache, reader);
                break;
            }
            case MODE: {
                // note the use of ClusteredCacheAdd.MODE
                ClusteredCacheResource.MODE.parseAndSetParameter(value, cache, reader);
                break;
            }
            case QUEUE_SIZE: {
                ClusteredCacheResource.QUEUE_SIZE.parseAndSetParameter(value, cache, reader);
                break;
            }
            case QUEUE_FLUSH_INTERVAL: {
                ClusteredCacheResource.QUEUE_FLUSH_INTERVAL.parseAndSetParameter(value, cache, reader);
                break;
            }
            case REMOTE_TIMEOUT: {
                ClusteredCacheResource.REMOTE_TIMEOUT.parseAndSetParameter(value, cache, reader);
                break;
            }
            default: {
                this.parseCacheAttribute(reader, index, attribute, value, cache);
            }
        }
    }

    private void parseLocalCache(XMLExtendedStreamReader reader, PathAddress containerAddress, List<ModelNode> operations) throws XMLStreamException {

        // ModelNode for the cache add operation
        ModelNode cache = Util.getEmptyOperation(ADD, null);
        // NOTE: this list is used to avoid lost attribute updates to the cache
        // object once it has been added to the operations list
        List<ModelNode> additionalConfigurationOperations = new ArrayList<ModelNode>();

        // set the cache mode to local
        // cache.get(ModelKeys.MODE).set(Configuration.CacheMode.LOCAL.name());

        for (int i = 0; i < reader.getAttributeCount(); i++) {
            String value = reader.getAttributeValue(i);
            Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
            this.parseCacheAttribute(reader, i, attribute, value, cache);
        }

        if (!cache.hasDefined(ModelKeys.NAME)) {
            throw ParseUtils.missingRequired(reader, EnumSet.of(Attribute.NAME));
        }

        // update the cache address with the cache name
        addNameToAddress(cache, containerAddress, ModelKeys.LOCAL_CACHE) ;

        while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
            Element element = Element.forName(reader.getLocalName());
            this.parseCacheElement(reader, element, cache, additionalConfigurationOperations);
        }

        operations.add(cache);
        // add operations to create configuration resources
        for (ModelNode additionalOperation : additionalConfigurationOperations) {
            operations.add(additionalOperation);
        }
    }

    private void parseDistributedCache(XMLExtendedStreamReader reader, PathAddress containerAddress, List<ModelNode> operations) throws XMLStreamException {

        // ModelNode for the cache add operation
        ModelNode cache = Util.getEmptyOperation(ModelDescriptionConstants.ADD, null);
        List<ModelNode> additionalConfigurationOperations = new ArrayList<ModelNode>();

        for (int i = 0; i < reader.getAttributeCount(); i++) {
            String value = reader.getAttributeValue(i);
            Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
            switch (attribute) {
                case OWNERS: {
                    DistributedCacheResource.OWNERS.parseAndSetParameter(value, cache, reader);
                    break;
                }
                case SEGMENTS: {
                    DistributedCacheResource.SEGMENTS.parseAndSetParameter(value, cache, reader);
                    break;
                }
               case CAPACITY_FACTOR: {
                  DistributedCacheResource.CAPACITY_FACTOR.parseAndSetParameter(value, cache, reader);
                  break;
               }
                case L1_LIFESPAN: {
                    DistributedCacheResource.L1_LIFESPAN.parseAndSetParameter(value, cache, reader);
                    break;
                }
                default: {
                    this.parseClusteredCacheAttribute(reader, i, attribute, value, cache);
                }
            }
        }

        if (!cache.hasDefined(ModelKeys.NAME)) {
            throw ParseUtils.missingRequired(reader, EnumSet.of(Attribute.NAME));
        }
        if (!cache.hasDefined(ModelKeys.MODE)) {
            throw ParseUtils.missingRequired(reader, EnumSet.of(Attribute.MODE));
        }

        // update the cache address with the cache name
        addNameToAddress(cache, containerAddress, ModelKeys.DISTRIBUTED_CACHE) ;

        while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
            Element element = Element.forName(reader.getLocalName());
            switch (element) {
                case STATE_TRANSFER: {
                    this.parseStateTransfer(reader, cache, additionalConfigurationOperations);
                    break;
                }
                default: {
                    this.parseCacheElement(reader, element, cache, additionalConfigurationOperations);
                }
            }
        }

        operations.add(cache);
        // add operations to create configuration resources
        for (ModelNode additionalOperation : additionalConfigurationOperations) {
            operations.add(additionalOperation);
        }
    }

    private void parseReplicatedCache(XMLExtendedStreamReader reader, PathAddress containerAddress, List<ModelNode> operations) throws XMLStreamException {

        // ModelNode for the cache add operation
        ModelNode cache = Util.getEmptyOperation(ModelDescriptionConstants.ADD, null);
        List<ModelNode> additionalConfigurationOperations = new ArrayList<ModelNode>();

        for (int i = 0; i < reader.getAttributeCount(); i++) {
            String value = reader.getAttributeValue(i);
            Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
            this.parseClusteredCacheAttribute(reader, i, attribute, value, cache);
        }

        if (!cache.hasDefined(ModelKeys.NAME)) {
            throw ParseUtils.missingRequired(reader, EnumSet.of(Attribute.NAME));
        }
        if (!cache.hasDefined(ModelKeys.MODE)) {
            throw ParseUtils.missingRequired(reader, EnumSet.of(Attribute.MODE));
        }

        // update the cache address with the cache name
        addNameToAddress(cache, containerAddress, ModelKeys.REPLICATED_CACHE) ;

        while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
            Element element = Element.forName(reader.getLocalName());
            switch (element) {
                case STATE_TRANSFER: {
                    this.parseStateTransfer(reader, cache, additionalConfigurationOperations);
                    break;
                }
                default: {
                    this.parseCacheElement(reader, element, cache, additionalConfigurationOperations);
                }
            }
        }

        operations.add(cache);
        // add operations to create configuration resources
        for (ModelNode additionalOperation : additionalConfigurationOperations) {
            operations.add(additionalOperation);
        }
    }

    private void parseInvalidationCache(XMLExtendedStreamReader reader, PathAddress containerAddress, List<ModelNode> operations) throws XMLStreamException {

        // ModelNode for the cache add operation
        ModelNode cache = Util.getEmptyOperation(ModelDescriptionConstants.ADD, null);
        List<ModelNode> additionalConfigurationOperations = new ArrayList<ModelNode>();

        for (int i = 0; i < reader.getAttributeCount(); i++) {
            String value = reader.getAttributeValue(i);
            Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
            this.parseClusteredCacheAttribute(reader, i, attribute, value, cache);
        }

        if (!cache.hasDefined(ModelKeys.NAME)) {
            throw ParseUtils.missingRequired(reader, EnumSet.of(Attribute.NAME));
        }
        if (!cache.hasDefined(ModelKeys.MODE)) {
            throw ParseUtils.missingRequired(reader, EnumSet.of(Attribute.MODE));
        }

        // update the cache address with the cache name
        addNameToAddress(cache, containerAddress, ModelKeys.INVALIDATION_CACHE) ;

        while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
            Element element = Element.forName(reader.getLocalName());
            switch (element) {
                default: {
                    this.parseCacheElement(reader, element, cache, additionalConfigurationOperations);
                }
            }
        }

        operations.add(cache);
        // add operations to create configuration resources
        for (ModelNode additionalOperation : additionalConfigurationOperations) {
            operations.add(additionalOperation);
        }
    }

    private void addNameToAddress(ModelNode current, PathAddress pathToOwner, String type) {

        String name = current.get(ModelKeys.NAME).asString();
        // setup the cache address
        PathAddress cacheAddress = pathToOwner.append(type, name);
        current.get(ModelDescriptionConstants.OP_ADDR).set(cacheAddress.toModelNode());

        // get rid of NAME now that we are finished with it
        current.remove(ModelKeys.NAME);
    }

    private void parseCacheElement(XMLExtendedStreamReader reader, Element element, ModelNode cache, List<ModelNode> operations) throws XMLStreamException {
        switch (element) {
            case BACKUPS: {
                this.parseBackups(reader, cache, operations);
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
                this.parseCacheSecurity(reader, cache, operations);
                break;
            }
            default: {
                throw ParseUtils.unexpectedElement(reader);
            }
        }
    }

    private void parseStateTransfer(XMLExtendedStreamReader reader, ModelNode cache, List<ModelNode> operations) throws XMLStreamException {

        PathAddress stateTransferAddress = PathAddress.pathAddress(cache.get(OP_ADDR)).append(ModelKeys.STATE_TRANSFER, ModelKeys.STATE_TRANSFER_NAME);
        ModelNode stateTransfer = Util.createAddOperation(stateTransferAddress);

        for (int i = 0; i < reader.getAttributeCount(); i++) {
            String value = reader.getAttributeValue(i);
            Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
            switch (attribute) {
                case AWAIT_INITIAL_TRANSFER: {
                    StateTransferResource.AWAIT_INITIAL_TRANSFER.parseAndSetParameter(value, stateTransfer, reader);
                    break;
                }
                case ENABLED: {
                    StateTransferResource.ENABLED.parseAndSetParameter(value, stateTransfer, reader);
                    break;
                }
                case TIMEOUT: {
                    StateTransferResource.TIMEOUT.parseAndSetParameter(value, stateTransfer, reader);
                    break;
                }
                case CHUNK_SIZE: {
                    StateTransferResource.CHUNK_SIZE.parseAndSetParameter(value, stateTransfer, reader);
                    break;
                }
                default: {
                    throw ParseUtils.unexpectedAttribute(reader, i);
                }
            }
        }
        ParseUtils.requireNoContent(reader);
        operations.add(stateTransfer);
    }

    private void parseLocking(XMLExtendedStreamReader reader, ModelNode cache, List<ModelNode> operations) throws XMLStreamException {

        PathAddress lockingAddress = PathAddress.pathAddress(cache.get(OP_ADDR)).append(ModelKeys.LOCKING, ModelKeys.LOCKING_NAME);
        ModelNode locking = Util.createAddOperation(lockingAddress);

        for (int i = 0; i < reader.getAttributeCount(); i++) {
            String value = reader.getAttributeValue(i);
            Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
            switch (attribute) {
                case ISOLATION: {
                    LockingResource.ISOLATION.parseAndSetParameter(value, locking, reader);
                    break;
                }
                case STRIPING: {
                    LockingResource.STRIPING.parseAndSetParameter(value, locking, reader);
                    break;
                }
                case ACQUIRE_TIMEOUT: {
                    LockingResource.ACQUIRE_TIMEOUT.parseAndSetParameter(value, locking, reader);
                    break;
                }
                case CONCURRENCY_LEVEL: {
                    LockingResource.CONCURRENCY_LEVEL.parseAndSetParameter(value, locking, reader);
                    break;
                }
                default: {
                    throw ParseUtils.unexpectedAttribute(reader, i);
                }
            }
        }
        ParseUtils.requireNoContent(reader);
        operations.add(locking);
    }

    private void parseTransaction(XMLExtendedStreamReader reader, ModelNode cache, List<ModelNode> operations) throws XMLStreamException {

        PathAddress transactionAddress = PathAddress.pathAddress(cache.get(OP_ADDR)).append(ModelKeys.TRANSACTION, ModelKeys.TRANSACTION_NAME);
        ModelNode transaction = Util.createAddOperation(transactionAddress);

        for (int i = 0; i < reader.getAttributeCount(); i++) {
            String value = reader.getAttributeValue(i);
            Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
            switch (attribute) {
                case STOP_TIMEOUT: {
                    TransactionResource.STOP_TIMEOUT.parseAndSetParameter(value, transaction, reader);
                    break;
                }
                case MODE: {
                    TransactionResource.MODE.parseAndSetParameter(value, transaction, reader);
                    break;
                }
                case LOCKING: {
                    TransactionResource.LOCKING.parseAndSetParameter(value, transaction, reader);
                    break;
                }
                default: {
                    throw ParseUtils.unexpectedAttribute(reader, i);
                }
            }
        }
        ParseUtils.requireNoContent(reader);
        operations.add(transaction);
    }

    private void parseEviction(XMLExtendedStreamReader reader, ModelNode cache, List<ModelNode> operations) throws XMLStreamException {

        PathAddress evictionAddress = PathAddress.pathAddress(cache.get(OP_ADDR)).append(ModelKeys.EVICTION, ModelKeys.EVICTION_NAME);
        ModelNode eviction = Util.createAddOperation(evictionAddress);

        for (int i = 0; i < reader.getAttributeCount(); i++) {
            String value = reader.getAttributeValue(i);
            Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
            switch (attribute) {
                case STRATEGY: {
                    EvictionResource.EVICTION_STRATEGY.parseAndSetParameter(value, eviction, reader);
                    break;
                }
                case MAX_ENTRIES: {
                    EvictionResource.MAX_ENTRIES.parseAndSetParameter(value, eviction, reader);
                    break;
                }
                default: {
                    throw ParseUtils.unexpectedAttribute(reader, i);
                }
            }
        }
        ParseUtils.requireNoContent(reader);
        operations.add(eviction);
    }

    private void parseExpiration(XMLExtendedStreamReader reader, ModelNode cache, List<ModelNode> operations) throws XMLStreamException {

        PathAddress expirationAddress = PathAddress.pathAddress(cache.get(OP_ADDR)).append(ModelKeys.EXPIRATION, ModelKeys.EXPIRATION_NAME);
        ModelNode expiration = Util.createAddOperation(expirationAddress);

        for (int i = 0; i < reader.getAttributeCount(); i++) {
            String value = reader.getAttributeValue(i);
            Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
            switch (attribute) {
                case MAX_IDLE: {
                    ExpirationResource.MAX_IDLE.parseAndSetParameter(value, expiration, reader);
                    break;
                }
                case LIFESPAN: {
                    ExpirationResource.LIFESPAN.parseAndSetParameter(value, expiration, reader);
                    break;
                }
                case INTERVAL: {
                    ExpirationResource.INTERVAL.parseAndSetParameter(value, expiration, reader);
                    break;
                }
                default: {
                    throw ParseUtils.unexpectedAttribute(reader, i);
                }
            }
        }
        ParseUtils.requireNoContent(reader);
        operations.add(expiration);
    }

    private void parseCustomLoader(XMLExtendedStreamReader reader, ModelNode cache, List<ModelNode> operations) throws XMLStreamException {

        ModelNode loader = Util.getEmptyOperation(ModelDescriptionConstants.ADD, null);
        String name = ModelKeys.LOADER_NAME;

        for (int i = 0; i < reader.getAttributeCount(); i++) {
            String value = reader.getAttributeValue(i);
            Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
            switch (attribute) {
                case CLASS: {
                    StoreResource.CLASS.parseAndSetParameter(value, loader, reader);
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

        // update the cache address with the loader name
        loader.get(ModelKeys.NAME).set(name);
        addNameToAddress(loader, PathAddress.pathAddress(cache.get(OP_ADDR)),
                         ModelKeys.LOADER);

        List<ModelNode> additionalConfigurationOperations = new ArrayList<ModelNode>();
        this.parseLoaderElements(reader, loader, additionalConfigurationOperations);
        operations.add(loader);
        operations.addAll(additionalConfigurationOperations);
    }

    private void parseClusterLoader(XMLExtendedStreamReader reader, ModelNode cache, List<ModelNode> operations) throws XMLStreamException {
        // ModelNode for the cluster loader add operation
        ModelNode loader = Util.getEmptyOperation(ModelDescriptionConstants.ADD, null);
        String name = ModelKeys.CLUSTER_LOADER_NAME;

        for (int i = 0; i < reader.getAttributeCount(); i++) {
            String value = reader.getAttributeValue(i);
            Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
            switch (attribute) {
                case REMOTE_TIMEOUT: {
                    ClusterLoaderResource.REMOTE_TIMEOUT.parseAndSetParameter(value, loader, reader);
                    break;
                }
                default: {
                    name = this.parseLoaderAttribute(name, reader, i, attribute, value, loader);
                }
            }
        }

        // update the cache address with the loader name
        loader.get(ModelKeys.NAME).set(name);
        addNameToAddress(loader, PathAddress.pathAddress(cache.get(OP_ADDR)),
                         ModelKeys.CLUSTER_LOADER);

        List<ModelNode> additionalConfigurationOperations = new ArrayList<ModelNode>();
        this.parseLoaderElements(reader, loader, additionalConfigurationOperations);
        operations.add(loader);
        operations.addAll(additionalConfigurationOperations);
    }

    private void parseCustomStore(XMLExtendedStreamReader reader, ModelNode cache, List<ModelNode> operations) throws XMLStreamException {

       ModelNode store = Util.getEmptyOperation(ModelDescriptionConstants.ADD, null);
       String name = ModelKeys.STORE_NAME;

        for (int i = 0; i < reader.getAttributeCount(); i++) {
            String value = reader.getAttributeValue(i);
            Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
            switch (attribute) {
                case CLASS: {
                    StoreResource.CLASS.parseAndSetParameter(value, store, reader);
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

        store.get(ModelKeys.NAME).set(name);
        addNameToAddress(store, PathAddress.pathAddress(cache.get(OP_ADDR)),
                         ModelKeys.STORE);

        List<ModelNode> additionalConfigurationOperations = new ArrayList<ModelNode>();
        this.parseStoreElements(reader, store, additionalConfigurationOperations);
        operations.add(store);
        operations.addAll(additionalConfigurationOperations);
    }

    private void parseFileStore(XMLExtendedStreamReader reader, ModelNode cache, List<ModelNode> operations) throws XMLStreamException {

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

        store.get(ModelKeys.NAME).set(name);
        addNameToAddress(store, PathAddress.pathAddress(cache.get(OP_ADDR)),
                         ModelKeys.FILE_STORE);

        List<ModelNode> additionalConfigurationOperations = new ArrayList<ModelNode>();
        this.parseStoreElements(reader, store, additionalConfigurationOperations);
        operations.add(store);
        operations.addAll(additionalConfigurationOperations);
    }

    private void parseRemoteStore(XMLExtendedStreamReader reader, ModelNode cache, List<ModelNode> operations) throws XMLStreamException {

       ModelNode store = Util.getEmptyOperation(ModelDescriptionConstants.ADD, null);
       String name = ModelKeys.REMOTE_STORE_NAME;

        List<ModelNode> additionalConfigurationOperations = new ArrayList<ModelNode>();

        for (int i = 0; i < reader.getAttributeCount(); i++) {
            String value = reader.getAttributeValue(i);
            Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
            switch (attribute) {
                case CACHE: {
                    RemoteStoreResource.CACHE.parseAndSetParameter(value, store, reader);
                    break;
                }
                case HOTROD_WRAPPING: {
                    RemoteStoreResource.HOTROD_WRAPPING.parseAndSetParameter(value, store, reader);
                    break;
                }
                case RAW_VALUES: {
                    RemoteStoreResource.RAW_VALUES.parseAndSetParameter(value, store, reader);
                    break;
                }
                case SOCKET_TIMEOUT: {
                    RemoteStoreResource.SOCKET_TIMEOUT.parseAndSetParameter(value, store, reader);
                    break;
                }
                case TCP_NO_DELAY: {
                    RemoteStoreResource.TCP_NO_DELAY.parseAndSetParameter(value, store, reader);
                    break;
                }

                default: {
                    name = this.parseStoreAttribute(name, reader, i, attribute, value, store);
                }
            }
        }

        store.get(ModelKeys.NAME).set(name);
        addNameToAddress(store, PathAddress.pathAddress(cache.get(OP_ADDR)),
                         ModelKeys.REMOTE_STORE);

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

        operations.add(store);
        operations.addAll(additionalConfigurationOperations);
    }

    private void parseLevelDBStore(XMLExtendedStreamReader reader, ModelNode cache, List<ModelNode> operations) throws XMLStreamException {
        ModelNode store = Util.getEmptyOperation(ModelDescriptionConstants.ADD, null);
        String name = ModelKeys.LEVELDB_STORE_NAME;

        List<ModelNode> additionalConfigurationOperations = new ArrayList<ModelNode>();

        for (int i = 0; i < reader.getAttributeCount(); i++) {
            String value = reader.getAttributeValue(i);
            Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
            switch (attribute) {
                case PATH: {
                    LevelDBStoreResource.PATH.parseAndSetParameter(value, store, reader);
                    break;
                }
                case BLOCK_SIZE: {
                    LevelDBStoreResource.BLOCK_SIZE.parseAndSetParameter(value, store, reader);
                    break;
                }
                case CACHE_SIZE: {
                    LevelDBStoreResource.CACHE_SIZE.parseAndSetParameter(value, store, reader);
                    break;
                }
                case CLEAR_THRESHOLD: {
                    LevelDBStoreResource.CLEAR_THRESHOLD.parseAndSetParameter(value, store, reader);
                    break;
                }
                default: {
                    name = this.parseStoreAttribute(name, reader, i, attribute, value, store);
                }
            }
        }

        store.get(ModelKeys.NAME).set(name);
        addNameToAddress(store, PathAddress.pathAddress(cache.get(OP_ADDR)),
                ModelKeys.LEVELDB_STORE);

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

        operations.add(store);
        operations.addAll(additionalConfigurationOperations);
    }

    private void parseStoreExpiry(XMLExtendedStreamReader reader, ModelNode store, List<ModelNode> operations) throws XMLStreamException {
        PathAddress storeExpiryAddress = PathAddress.pathAddress(store.get(OP_ADDR)).append(ModelKeys.EXPIRATION, ModelKeys.EXPIRATION_NAME);
        ModelNode storeExpiry = Util.createAddOperation(storeExpiryAddress);

        for (int i = 0; i < reader.getAttributeCount(); i++) {
            String value = reader.getAttributeValue(i);
            Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
            switch (attribute) {
                case PATH: {
                    LevelDBExpirationResource.PATH.parseAndSetParameter(value, storeExpiry, reader);
                    break;
                }
                case QUEUE_SIZE: {
                    LevelDBExpirationResource.QUEUE_SIZE.parseAndSetParameter(value, storeExpiry, reader);
                    break;
                }
                default:
                    throw ParseUtils.unexpectedAttribute(reader, i);
            }
        }
        ParseUtils.requireNoContent(reader);
        operations.add(storeExpiry);
    }

    private void parseStoreCompression(XMLExtendedStreamReader reader, ModelNode store, List<ModelNode> operations) throws XMLStreamException {
        PathAddress storeCompressionAddress = PathAddress.pathAddress(store.get(OP_ADDR)).append(ModelKeys.COMPRESSION, ModelKeys.COMPRESSION_NAME);
        ModelNode storeCompression = Util.createAddOperation(storeCompressionAddress);

        for (int i = 0; i < reader.getAttributeCount(); i++) {
            String value = reader.getAttributeValue(i);
            Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
            switch (attribute) {
                case TYPE: {
                    LevelDBCompressionResource.TYPE.parseAndSetParameter(value, storeCompression, reader);
                    break;
                }
                default:
                    throw ParseUtils.unexpectedAttribute(reader, i);
            }
        }
        ParseUtils.requireNoContent(reader);
        operations.add(storeCompression);
    }

    private void parseStoreImplementation(XMLExtendedStreamReader reader, ModelNode store, List<ModelNode> operations) throws XMLStreamException {
        PathAddress storeImplementationAddress = PathAddress.pathAddress(store.get(OP_ADDR)).append(ModelKeys.IMPLEMENTATION, ModelKeys.IMPLEMENTATION_NAME);
        ModelNode storeImplementation = Util.createAddOperation(storeImplementationAddress);

        for (int i = 0; i < reader.getAttributeCount(); i++) {
            String value = reader.getAttributeValue(i);
            Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
            switch (attribute) {
                case TYPE: {
                    LevelDBImplementationResource.TYPE.parseAndSetParameter(value, storeImplementation, reader);
                    break;
                }
                default:
                    throw ParseUtils.unexpectedAttribute(reader, i);
            }
        }
        ParseUtils.requireNoContent(reader);
        operations.add(storeImplementation);
    }

    private void parseRemoteServer(XMLExtendedStreamReader reader, ModelNode server) throws XMLStreamException {
        for (int i = 0; i < reader.getAttributeCount(); i++) {
            String value = reader.getAttributeValue(i);
            Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
            switch (attribute) {
                case OUTBOUND_SOCKET_BINDING: {
                    RemoteStoreResource.OUTBOUND_SOCKET_BINDING.parseAndSetParameter(value, server, reader);
                    break;
                }
                default: {
                    throw ParseUtils.unexpectedAttribute(reader, i);
                }
            }
        }
        ParseUtils.requireNoContent(reader);
    }

    private void parseRestStore(XMLExtendedStreamReader reader, ModelNode cache, List<ModelNode> operations) throws XMLStreamException {

        ModelNode store = Util.getEmptyOperation(ModelDescriptionConstants.ADD, null);
        String name = ModelKeys.REST_STORE_NAME;

         List<ModelNode> additionalConfigurationOperations = new ArrayList<ModelNode>();

         for (int i = 0; i < reader.getAttributeCount(); i++) {
             String value = reader.getAttributeValue(i);
             Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
             switch (attribute) {
                 case APPEND_CACHE_NAME_TO_PATH: {
                     RestStoreResource.APPEND_CACHE_NAME_TO_PATH.parseAndSetParameter(value, store, reader);
                     break;
                 }
                 case PATH: {
                     RestStoreResource.PATH.parseAndSetParameter(value, store, reader);
                     break;
                 }

                 default: {
                     name = this.parseStoreAttribute(name, reader, i, attribute, value, store);
                 }
             }
         }

         store.get(ModelKeys.NAME).set(name);
         addNameToAddress(store, PathAddress.pathAddress(cache.get(OP_ADDR)),
                          ModelKeys.REST_STORE);

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

         operations.add(store);
         operations.addAll(additionalConfigurationOperations);
     }

    private void parseRestConnectionPool(XMLExtendedStreamReader reader, ModelNode table) throws XMLStreamException {
        for (int i = 0; i < reader.getAttributeCount(); i++) {
            String value = reader.getAttributeValue(i);
            Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
            switch (attribute) {
                case BUFFER_SIZE: {
                    RestStoreResource.BUFFER_SIZE.parseAndSetParameter(value, table, reader);
                    break;
                }
                case CONNECTION_TIMEOUT: {
                    RestStoreResource.CONNECTION_TIMEOUT.parseAndSetParameter(value, table, reader);
                    break;
                }
                case MAX_CONNECTIONS_PER_HOST: {
                    RestStoreResource.MAX_CONNECTIONS_PER_HOST.parseAndSetParameter(value, table, reader);
                    break;
                }
                case MAX_TOTAL_CONNECTIONS: {
                    RestStoreResource.MAX_TOTAL_CONNECTIONS.parseAndSetParameter(value, table, reader);
                    break;
                }
                case SOCKET_TIMEOUT: {
                    RestStoreResource.SOCKET_TIMEOUT.parseAndSetParameter(value, table, reader);
                    break;
                }
                case TCP_NO_DELAY: {
                    RestStoreResource.TCP_NO_DELAY.parseAndSetParameter(value, table, reader);
                    break;
                }
                default: {
                    throw ParseUtils.unexpectedAttribute(reader, i);
                }
            }
        }
        ParseUtils.requireNoContent(reader);
    }

    private void parseStringKeyedJDBCStore(XMLExtendedStreamReader reader, ModelNode cache, List<ModelNode> operations) throws XMLStreamException {

       ModelNode store = Util.getEmptyOperation(ModelDescriptionConstants.ADD, null);
       String name = ModelKeys.STRING_KEYED_JDBC_STORE_NAME;

        List<ModelNode> additionalConfigurationOperations = new ArrayList<ModelNode>();

        for (int i = 0; i < reader.getAttributeCount(); i++) {
            String value = reader.getAttributeValue(i);
            Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
            switch (attribute) {
                case DATASOURCE: {
                    BaseJDBCStoreResource.DATA_SOURCE.parseAndSetParameter(value, store, reader);
                    break;
                }
                case DIALECT: {
                    BaseJDBCStoreResource.DIALECT.parseAndSetParameter(value, store, reader);
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

        store.get(ModelKeys.NAME).set(name);
        addNameToAddress(store, PathAddress.pathAddress(cache.get(OP_ADDR)),
                         ModelKeys.STRING_KEYED_JDBC_STORE);

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

        operations.add(store);
        operations.addAll(additionalConfigurationOperations);
    }

    private void parseBinaryKeyedJDBCStore(XMLExtendedStreamReader reader, ModelNode cache, List<ModelNode> operations) throws XMLStreamException {

        ModelNode store = Util.getEmptyOperation(ModelDescriptionConstants.ADD, null);
        String name = ModelKeys.BINARY_KEYED_JDBC_STORE_NAME;

        List<ModelNode> additionalConfigurationOperations = new ArrayList<ModelNode>();

        for (int i = 0; i < reader.getAttributeCount(); i++) {
            String value = reader.getAttributeValue(i);
            Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
            switch (attribute) {
                case DATASOURCE: {
                    BaseJDBCStoreResource.DATA_SOURCE.parseAndSetParameter(value, store, reader);
                    break;
                }
                case DIALECT: {
                    BaseJDBCStoreResource.DIALECT.parseAndSetParameter(value, store, reader);
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

        store.get(ModelKeys.NAME).set(name);
        addNameToAddress(store, PathAddress.pathAddress(cache.get(OP_ADDR)),
                         ModelKeys.BINARY_KEYED_JDBC_STORE);

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

        operations.add(store);
        operations.addAll(additionalConfigurationOperations);
    }
    private void parseMixedKeyedJDBCStore(XMLExtendedStreamReader reader, ModelNode cache, List<ModelNode> operations) throws XMLStreamException {

        ModelNode store = Util.getEmptyOperation(ModelDescriptionConstants.ADD, null);
        String name = ModelKeys.MIXED_KEYED_JDBC_STORE_NAME;

        List<ModelNode> additionalConfigurationOperations = new ArrayList<ModelNode>();

        for (int i = 0; i < reader.getAttributeCount(); i++) {
            String value = reader.getAttributeValue(i);
            Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
            switch (attribute) {
                case DATASOURCE: {
                    BaseJDBCStoreResource.DATA_SOURCE.parseAndSetParameter(value, store, reader);
                    break;
                }
                case DIALECT: {
                    BaseJDBCStoreResource.DIALECT.parseAndSetParameter(value, store, reader);
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

        store.get(ModelKeys.NAME).set(name);
        addNameToAddress(store, PathAddress.pathAddress(cache.get(OP_ADDR)),
                         ModelKeys.MIXED_KEYED_JDBC_STORE);

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

        operations.add(store);
        operations.addAll(additionalConfigurationOperations);
    }

    private void parseJDBCStoreTable(XMLExtendedStreamReader reader, ModelNode table) throws XMLStreamException {
        for (int i = 0; i < reader.getAttributeCount(); i++) {
            String value = reader.getAttributeValue(i);
            Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
            switch (attribute) {
                case PREFIX: {
                    BaseJDBCStoreResource.PREFIX.parseAndSetParameter(value, table, reader);
                    break;
                }
                case FETCH_SIZE: {
                    BaseJDBCStoreResource.FETCH_SIZE.parseAndSetParameter(value, table, reader);
                    break;
                }
                case BATCH_SIZE: {
                    BaseJDBCStoreResource.BATCH_SIZE.parseAndSetParameter(value, table, reader);
                    break;
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
                    BaseJDBCStoreResource.COLUMN_NAME.parseAndSetParameter(value, column, reader);
                    break;
                }
                case TYPE: {
                    BaseJDBCStoreResource.COLUMN_TYPE.parseAndSetParameter(value, column, reader);
                    break;
                }
                default: {
                    throw ParseUtils.unexpectedAttribute(reader, i);
                }
            }
        }
        ParseUtils.requireNoContent(reader);
    }

    private void parseLoaderElements(XMLExtendedStreamReader reader, ModelNode loader, List<ModelNode> operations) throws XMLStreamException {
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
                BaseLoaderResource.NAME.parseAndSetParameter(value, loader, reader);
                break;
            }
            case SHARED: {
                BaseLoaderResource.SHARED.parseAndSetParameter(value, loader, reader);
                break;
            }
            case PRELOAD: {
                BaseLoaderResource.PRELOAD.parseAndSetParameter(value, loader, reader);
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
                BaseStoreResource.NAME.parseAndSetParameter(value, store, reader);
                break;
            }
            case SHARED: {
                BaseStoreResource.SHARED.parseAndSetParameter(value, store, reader);
                break;
            }
            case PRELOAD: {
                BaseStoreResource.PRELOAD.parseAndSetParameter(value, store, reader);
                break;
            }
            case PASSIVATION: {
                BaseStoreResource.PASSIVATION.parseAndSetParameter(value, store, reader);
                break;
            }
            case FETCH_STATE: {
                BaseStoreResource.FETCH_STATE.parseAndSetParameter(value, store, reader);
                break;
            }
            case PURGE: {
                BaseStoreResource.PURGE.parseAndSetParameter(value, store, reader);
                break;
            }
            case SINGLETON: {
                BaseStoreResource.SINGLETON.parseAndSetParameter(value, store, reader);
                break;
            }
            case READ_ONLY: {
                BaseStoreResource.READ_ONLY.parseAndSetParameter(value, store, reader);
                break;
            }
            default: {
                throw ParseUtils.unexpectedAttribute(reader, index);
            }
        }
        return name;
    }

    private void parseStoreElements(XMLExtendedStreamReader reader, ModelNode store, List<ModelNode> operations) throws XMLStreamException {
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

    private void parseStoreWriteBehind(XMLExtendedStreamReader reader, ModelNode store, List<ModelNode> operations) throws XMLStreamException {

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
        operations.add(writeBehind);
    }

    private void parseStoreProperty(XMLExtendedStreamReader reader, ModelNode node, final List<ModelNode> operations) throws XMLStreamException {

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

        operations.add(property);
    }

    private void parseIndexing(XMLExtendedStreamReader reader, ModelNode node) throws XMLStreamException {
        for (int i = 0; i < reader.getAttributeCount(); i++) {
            String value = reader.getAttributeValue(i);
            Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
            if (attribute == Attribute.INDEX) {
                CacheResource.INDEXING.parseAndSetParameter(value, node, reader);
            } else {
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
                    CacheResource.INDEXING_PROPERTIES.parseAndAddParameterElement(property, value, node, reader);
                    break;
                }
                default: {
                    throw ParseUtils.unexpectedElement(reader);
                }
            }
        }
       // ParseUtils.requireNoContent(reader);
    }

    private void parseBackups(XMLExtendedStreamReader reader, ModelNode cache, List<ModelNode> operations) throws XMLStreamException {

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

    private void parseBackup(XMLExtendedStreamReader reader, ModelNode cache, List<ModelNode> operations) throws XMLStreamException {

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
                    BackupSiteResource.STRATEGY.parseAndSetParameter(value, operation, reader);
                    break;
                }
                case BACKUP_FAILURE_POLICY: {
                    BackupSiteResource.FAILURE_POLICY.parseAndSetParameter(value, operation, reader);
                    break;
                }
                case TIMEOUT: {
                    BackupSiteResource.REPLICATION_TIMEOUT.parseAndSetParameter(value, operation, reader);
                    break;
                }
                case ENABLED: {
                    BackupSiteResource.ENABLED.parseAndSetParameter(value, operation, reader);
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

        while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
            Element element = Element.forName(reader.getLocalName());
            switch (element) {
                case TAKE_OFFLINE: {
                    this.parseTakeOffline(reader, operation);
                    break;
                }
                default: {
                    throw ParseUtils.unexpectedElement(reader);
                }
            }
        }

        operations.add(operation);
    }

    private void parseTakeOffline(XMLExtendedStreamReader reader, ModelNode operation) throws XMLStreamException {
        for (int i = 0; i < reader.getAttributeCount(); i++) {
            String value = reader.getAttributeValue(i);
            Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
            switch (attribute) {
                case TAKE_BACKUP_OFFLINE_AFTER_FAILURES: {
                    BackupSiteResource.TAKE_OFFLINE_AFTER_FAILURES.parseAndSetParameter(value, operation, reader);
                    break;
                }
                case TAKE_BACKUP_OFFLINE_MIN_WAIT: {
                    BackupSiteResource.TAKE_OFFLINE_MIN_WAIT.parseAndSetParameter(value, operation, reader);
                    break;
                }
                default: {
                    throw ParseUtils.unexpectedAttribute(reader, i);
                }
            }
        }
        ParseUtils.requireNoContent(reader);
    }

    private void parseCompatibility(XMLExtendedStreamReader reader, ModelNode cache, List<ModelNode> operations) throws XMLStreamException {
        PathAddress compatibilityAddress = PathAddress.pathAddress(cache.get(OP_ADDR)).append(ModelKeys.COMPATIBILITY, ModelKeys.COMPATIBILITY_NAME);
        ModelNode compatibility = Util.createAddOperation(compatibilityAddress);

        for (int i = 0; i < reader.getAttributeCount(); i++) {
            String value = reader.getAttributeValue(i);
            Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
            switch (attribute) {
                case ENABLED: {
                    CompatibilityResource.ENABLED.parseAndSetParameter(value, compatibility, reader);
                    break;
                }
                case MARSHALLER: {
                    CompatibilityResource.MARSHALLER.parseAndSetParameter(value, compatibility, reader);
                    break;
                }
                default: {
                    throw ParseUtils.unexpectedAttribute(reader, i);
                }
            }
        }
        ParseUtils.requireNoContent(reader);
        operations.add(compatibility);
    }

    private void parseCacheSecurity(XMLExtendedStreamReader reader, ModelNode cache, List<ModelNode> operations) throws XMLStreamException {
        PathAddress securityAddress = PathAddress.pathAddress(cache.get(OP_ADDR)).append(ModelKeys.SECURITY, ModelKeys.SECURITY_NAME);
        ModelNode security = Util.createAddOperation(securityAddress);

        ParseUtils.requireNoAttributes(reader);
        List<ModelNode> additionalConfigurationOperations = new ArrayList<ModelNode>();

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
        operations.add(security);
        // add operations to create configuration resources
        for (ModelNode additionalOperation : additionalConfigurationOperations) {
            operations.add(additionalOperation);
        }
    }

    private void parseCacheAuthorization(XMLExtendedStreamReader reader, ModelNode security, List<ModelNode> operations) throws XMLStreamException {
        PathAddress authorizationAddress = PathAddress.pathAddress(security.get(OP_ADDR)).append(ModelKeys.AUTHORIZATION, ModelKeys.AUTHORIZATION_NAME);
        ModelNode authorization = Util.createAddOperation(authorizationAddress);

        for (int i = 0; i < reader.getAttributeCount(); i++) {
            String value = reader.getAttributeValue(i);
            Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
            switch (attribute) {
                case ENABLED: {
                    CacheAuthorizationResource.ENABLED.parseAndSetParameter(value, authorization, reader);
                    break;
                }
                case ROLES: {
                    for(String role : reader.getListAttributeValue(i)) {
                        CacheAuthorizationResource.ROLES.parseAndAddParameterElement(role, authorization, reader);
                    }
                    break;
                }
                default: {
                    throw ParseUtils.unexpectedAttribute(reader, i);
                }
            }
        }

        ParseUtils.requireNoContent(reader);
        operations.add(authorization);
    }


}
