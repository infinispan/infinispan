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

import org.infinispan.persistence.rest.configuration.ConnectionPoolConfiguration;
import org.infinispan.persistence.rest.configuration.RestStoreConfiguration;
import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.ObjectListAttributeDefinition;
import org.jboss.as.controller.ObjectTypeAttributeDefinition;
import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.SimpleAttributeDefinition;
import org.jboss.as.controller.SimpleAttributeDefinitionBuilder;
import org.jboss.as.controller.client.helpers.MeasurementUnit;
import org.jboss.as.controller.registry.AttributeAccess;
import org.jboss.as.controller.registry.ManagementResourceRegistration;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;

/**
 * Resource description for the addressable resource /subsystem=infinispan/cache-container=X/cache=Y/rest-store=REST_STORE
 *
 * @author Tristan Tarrant
 */
public class RestStoreConfigurationResource extends BaseStoreConfigurationResource {

    public static final PathElement REST_STORE_PATH = PathElement.pathElement(ModelKeys.REST_STORE);

    // attributes
    static final SimpleAttributeDefinition PATH =
            new SimpleAttributeDefinitionBuilder(ModelKeys.PATH, ModelType.STRING, true)
                    .setXmlName(Attribute.PATH.getLocalName())
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                    .setDefaultValue(new ModelNode().set("/rest/___defaultcache"))
                    .build();
    static final SimpleAttributeDefinition APPEND_CACHE_NAME_TO_PATH =
            new SimpleAttributeDefinitionBuilder(ModelKeys.APPEND_CACHE_NAME_TO_PATH, ModelType.BOOLEAN, true)
                    .setXmlName(Attribute.APPEND_CACHE_NAME_TO_PATH.getLocalName())
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                    .setDefaultValue(new ModelNode().set(RestStoreConfiguration.APPEND_CACHE_NAME_TO_PATH.getDefaultValue()))
                    .build();
    static final SimpleAttributeDefinition MAX_CONTENT_LENGTH =
          new SimpleAttributeDefinitionBuilder(ModelKeys.MAX_CONTENT_LENGTH, ModelType.INT, true)
                .setXmlName(Attribute.MAX_CONTENT_LENGTH.getLocalName())
                .setAllowExpression(true)
                .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                .setDefaultValue(new ModelNode().set(RestStoreConfiguration.MAX_CONTENT_LENGTH.getDefaultValue()))
                .build();
    // connection pool attributes
    static final SimpleAttributeDefinition BUFFER_SIZE =
            new SimpleAttributeDefinitionBuilder(ModelKeys.BUFFER_SIZE, ModelType.INT, true)
                    .setXmlName(Attribute.BUFFER_SIZE.getLocalName())
                    .setMeasurementUnit(MeasurementUnit.BYTES)
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                    .setDefaultValue(new ModelNode().set(ConnectionPoolConfiguration.BUFFER_SIZE.getDefaultValue()))
                    .build();
    static final SimpleAttributeDefinition CONNECTION_TIMEOUT =
            new SimpleAttributeDefinitionBuilder(ModelKeys.CONNECTION_TIMEOUT, ModelType.INT, true)
                    .setXmlName(Attribute.CONNECTION_TIMEOUT.getLocalName())
                    .setMeasurementUnit(MeasurementUnit.MILLISECONDS)
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                    .setDefaultValue(new ModelNode().set(ConnectionPoolConfiguration.CONNECTION_TIMEOUT.getDefaultValue()))
                    .build();
    static final SimpleAttributeDefinition MAX_CONNECTIONS_PER_HOST =
            new SimpleAttributeDefinitionBuilder(ModelKeys.MAX_CONNECTIONS_PER_HOST, ModelType.INT, true)
                    .setXmlName(Attribute.MAX_CONNECTIONS_PER_HOST.getLocalName())
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                    .setDefaultValue(new ModelNode().set(ConnectionPoolConfiguration.MAX_CONNECTIONS_PER_HOST.getDefaultValue()))
                    .build();
    static final SimpleAttributeDefinition MAX_TOTAL_CONNECTIONS =
            new SimpleAttributeDefinitionBuilder(ModelKeys.MAX_TOTAL_CONNECTIONS, ModelType.INT, true)
                    .setXmlName(Attribute.MAX_TOTAL_CONNECTIONS.getLocalName())
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                    .setDefaultValue(new ModelNode().set(ConnectionPoolConfiguration.MAX_TOTAL_CONNECTIONS.getDefaultValue()))
                    .build();
    static final SimpleAttributeDefinition SOCKET_TIMEOUT =
            new SimpleAttributeDefinitionBuilder(ModelKeys.SOCKET_TIMEOUT, ModelType.INT, true)
                    .setXmlName(Attribute.SOCKET_TIMEOUT.getLocalName())
                    .setMeasurementUnit(MeasurementUnit.MILLISECONDS)
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                    .setDefaultValue(new ModelNode().set(ConnectionPoolConfiguration.SOCKET_TIMEOUT.getDefaultValue()))
                    .build();
    static final SimpleAttributeDefinition TCP_NO_DELAY =
            new SimpleAttributeDefinitionBuilder(ModelKeys.TCP_NO_DELAY, ModelType.BOOLEAN, true)
                    .setXmlName(Attribute.TCP_NO_DELAY.getLocalName())
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                    .setDefaultValue(new ModelNode().set(ConnectionPoolConfiguration.TCP_NO_DELAY.getDefaultValue()))
                    .build();
    static final ObjectTypeAttributeDefinition CONNECTION_POOL = ObjectTypeAttributeDefinition.
            Builder.of(ModelKeys.CONNECTION_POOL, BUFFER_SIZE, CONNECTION_TIMEOUT, MAX_CONNECTIONS_PER_HOST, MAX_TOTAL_CONNECTIONS, SOCKET_TIMEOUT, TCP_NO_DELAY).
            setRequired(false).
            build();

    // remote server attributes
    static final SimpleAttributeDefinition OUTBOUND_SOCKET_BINDING = new SimpleAttributeDefinitionBuilder(ModelKeys.OUTBOUND_SOCKET_BINDING, ModelType.STRING, true).build();

    static final SimpleAttributeDefinition NAME =
            new SimpleAttributeDefinitionBuilder(BaseStoreConfigurationResource.NAME)
                   .setDefaultValue(new ModelNode().set(ModelKeys.REST_STORE_NAME))
                   .build();

    static final ObjectTypeAttributeDefinition REMOTE_SERVER = ObjectTypeAttributeDefinition.
            Builder.of(ModelKeys.REMOTE_SERVER, OUTBOUND_SOCKET_BINDING).
            setRequired(false).
            setSuffix(ModelKeys.REMOTE_SERVER).
            build();

    static final ObjectListAttributeDefinition REMOTE_SERVERS = ObjectListAttributeDefinition.Builder.of(ModelKeys.REMOTE_SERVERS, REMOTE_SERVER).
            setRequired(true).
            build();

    static final AttributeDefinition[] REST_STORE_ATTRIBUTES = {PATH, APPEND_CACHE_NAME_TO_PATH, MAX_CONTENT_LENGTH, CONNECTION_POOL, REMOTE_SERVERS};

    public RestStoreConfigurationResource(CacheConfigurationResource parent, ManagementResourceRegistration containerReg) {
        super(REST_STORE_PATH, ModelKeys.REST_STORE, parent, containerReg, REST_STORE_ATTRIBUTES);
    }
}
