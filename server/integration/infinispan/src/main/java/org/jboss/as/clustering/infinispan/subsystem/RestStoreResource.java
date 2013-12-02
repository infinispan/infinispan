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

import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.ADD;

import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.ObjectListAttributeDefinition;
import org.jboss.as.controller.ObjectTypeAttributeDefinition;
import org.jboss.as.controller.OperationDefinition;
import org.jboss.as.controller.OperationStepHandler;
import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.ReloadRequiredRemoveStepHandler;
import org.jboss.as.controller.ReloadRequiredWriteAttributeHandler;
import org.jboss.as.controller.SimpleAttributeDefinition;
import org.jboss.as.controller.SimpleAttributeDefinitionBuilder;
import org.jboss.as.controller.SimpleOperationDefinitionBuilder;
import org.jboss.as.controller.client.helpers.MeasurementUnit;
import org.jboss.as.controller.registry.AttributeAccess;
import org.jboss.as.controller.registry.ManagementResourceRegistration;
import org.jboss.as.controller.registry.OperationEntry;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;

/**
 * Resource description for the addressable resource /subsystem=infinispan/cache-container=X/cache=Y/rest-store=REST_STORE
 *
 * @author Tristan Tarrant
 */
public class RestStoreResource extends BaseStoreResource {

    public static final PathElement REST_STORE_PATH = PathElement.pathElement(ModelKeys.REST_STORE);

    // attributes
    static final SimpleAttributeDefinition PATH =
            new SimpleAttributeDefinitionBuilder(ModelKeys.PATH, ModelType.STRING, true)
                    .setXmlName(Attribute.PATH.getLocalName())
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_ALL_SERVICES)
                    .setDefaultValue(new ModelNode().set("/rest/___defaultcache"))
                    .build();
    static final SimpleAttributeDefinition APPEND_CACHE_NAME_TO_PATH =
            new SimpleAttributeDefinitionBuilder(ModelKeys.APPEND_CACHE_NAME_TO_PATH, ModelType.BOOLEAN, true)
                    .setXmlName(Attribute.APPEND_CACHE_NAME_TO_PATH.getLocalName())
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_ALL_SERVICES)
                    .setDefaultValue(new ModelNode().set(false))
                    .build();
    // connection pool attributes
    static final SimpleAttributeDefinition BUFFER_SIZE =
            new SimpleAttributeDefinitionBuilder(ModelKeys.BUFFER_SIZE, ModelType.INT, true)
                    .setXmlName(Attribute.BUFFER_SIZE.getLocalName())
                    .setMeasurementUnit(MeasurementUnit.BYTES)
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_ALL_SERVICES)
                    .setDefaultValue(new ModelNode().set(8192))
                    .build();
    static final SimpleAttributeDefinition CONNECTION_TIMEOUT =
            new SimpleAttributeDefinitionBuilder(ModelKeys.CONNECTION_TIMEOUT, ModelType.INT, true)
                    .setXmlName(Attribute.CONNECTION_TIMEOUT.getLocalName())
                    .setMeasurementUnit(MeasurementUnit.MILLISECONDS)
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_ALL_SERVICES)
                    .setDefaultValue(new ModelNode().set(60000))
                    .build();
    static final SimpleAttributeDefinition MAX_CONNECTIONS_PER_HOST =
            new SimpleAttributeDefinitionBuilder(ModelKeys.MAX_CONNECTIONS_PER_HOST, ModelType.INT, true)
                    .setXmlName(Attribute.MAX_CONNECTIONS_PER_HOST.getLocalName())
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_ALL_SERVICES)
                    .setDefaultValue(new ModelNode().set(4))
                    .build();
    static final SimpleAttributeDefinition MAX_TOTAL_CONNECTIONS =
            new SimpleAttributeDefinitionBuilder(ModelKeys.MAX_TOTAL_CONNECTIONS, ModelType.INT, true)
                    .setXmlName(Attribute.MAX_TOTAL_CONNECTIONS.getLocalName())
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_ALL_SERVICES)
                    .setDefaultValue(new ModelNode().set(20))
                    .build();
    static final SimpleAttributeDefinition SOCKET_TIMEOUT =
            new SimpleAttributeDefinitionBuilder(ModelKeys.SOCKET_TIMEOUT, ModelType.INT, true)
                    .setXmlName(Attribute.SOCKET_TIMEOUT.getLocalName())
                    .setMeasurementUnit(MeasurementUnit.MILLISECONDS)
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_ALL_SERVICES)
                    .setDefaultValue(new ModelNode().set(60000))
                    .build();
    static final SimpleAttributeDefinition TCP_NO_DELAY =
            new SimpleAttributeDefinitionBuilder(ModelKeys.TCP_NO_DELAY, ModelType.BOOLEAN, true)
                    .setXmlName(Attribute.TCP_NO_DELAY.getLocalName())
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_ALL_SERVICES)
                    .setDefaultValue(new ModelNode().set(true))
                    .build();
    static final ObjectTypeAttributeDefinition CONNECTION_POOL = ObjectTypeAttributeDefinition.
            Builder.of(ModelKeys.CONNECTION_POOL, BUFFER_SIZE, CONNECTION_TIMEOUT, MAX_CONNECTIONS_PER_HOST, MAX_TOTAL_CONNECTIONS, SOCKET_TIMEOUT, TCP_NO_DELAY).
            setAllowNull(true).
            build();

    // remote server attributes
    static final SimpleAttributeDefinition OUTBOUND_SOCKET_BINDING = new SimpleAttributeDefinition("outbound-socket-binding", ModelType.STRING, true);

    static final SimpleAttributeDefinition NAME =
            new SimpleAttributeDefinitionBuilder(BaseStoreResource.NAME)
                   .setDefaultValue(new ModelNode().set(ModelKeys.REST_STORE_NAME))
                   .build();

    static final ObjectTypeAttributeDefinition REMOTE_SERVER = ObjectTypeAttributeDefinition.
            Builder.of(ModelKeys.REMOTE_SERVER, OUTBOUND_SOCKET_BINDING).
            setAllowNull(true).
            setSuffix("remote-server").
            build();

    static final ObjectListAttributeDefinition REMOTE_SERVERS = ObjectListAttributeDefinition.Builder.of(ModelKeys.REMOTE_SERVERS, REMOTE_SERVER).
            setAllowNull(false).
            build();

    static final AttributeDefinition[] REST_STORE_ATTRIBUTES = {PATH, APPEND_CACHE_NAME_TO_PATH, CONNECTION_POOL, REMOTE_SERVERS};

    // operations
    private static final OperationDefinition REST_STORE_ADD_DEFINITION = new SimpleOperationDefinitionBuilder(ADD, InfinispanExtension.getResourceDescriptionResolver(ModelKeys.REST_STORE))
        .setParameters(COMMON_STORE_PARAMETERS)
        .addParameter(PATH)
        .addParameter(APPEND_CACHE_NAME_TO_PATH)
        .addParameter(CONNECTION_POOL)
        .addParameter(REMOTE_SERVERS)
        .setAttributeResolver(InfinispanExtension.getResourceDescriptionResolver(ModelKeys.REST_STORE))
        .build();

    public RestStoreResource() {
        super(REST_STORE_PATH,
                InfinispanExtension.getResourceDescriptionResolver(ModelKeys.REST_STORE),
                CacheConfigOperationHandlers.REST_STORE_ADD,
                ReloadRequiredRemoveStepHandler.INSTANCE);
    }

    @Override
    public void registerAttributes(ManagementResourceRegistration resourceRegistration) {
        super.registerAttributes(resourceRegistration);

        // check that we don't need a special handler here?
        final OperationStepHandler writeHandler = new ReloadRequiredWriteAttributeHandler(REST_STORE_ATTRIBUTES);
        for (AttributeDefinition attr : REST_STORE_ATTRIBUTES) {
            resourceRegistration.registerReadWriteAttribute(attr, null, writeHandler);
        }
    }

    @Override
    public void registerOperations(ManagementResourceRegistration resourceRegistration) {
        super.registerOperations(resourceRegistration);
    }

    // override the add operation to provide a custom definition (for the optional PROPERTIES parameter to add())
    @Override
    protected void registerAddOperation(final ManagementResourceRegistration registration, final OperationStepHandler handler, OperationEntry.Flag... flags) {
        registration.registerOperationHandler(REST_STORE_ADD_DEFINITION, handler);
    }
}
