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

import org.infinispan.client.hotrod.ProtocolVersion;
import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.ObjectListAttributeDefinition;
import org.jboss.as.controller.ObjectTypeAttributeDefinition;
import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.SimpleAttributeDefinition;
import org.jboss.as.controller.SimpleAttributeDefinitionBuilder;
import org.jboss.as.controller.client.helpers.MeasurementUnit;
import org.jboss.as.controller.operations.validation.EnumValidator;
import org.jboss.as.controller.registry.AttributeAccess;
import org.jboss.as.controller.registry.ManagementResourceRegistration;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;

/**
 * Resource description for the addressable resource /subsystem=infinispan/cache-container=X/cache=Y/remote-store=REMOTE_STORE
 *
 * @author Richard Achmatowicz (c) 2011 Red Hat Inc.
 */
public class RemoteStoreConfigurationResource extends BaseStoreConfigurationResource {

    public static final PathElement REMOTE_STORE_PATH = PathElement.pathElement(ModelKeys.REMOTE_STORE);

    // attributes
    static final SimpleAttributeDefinition CACHE =
            new SimpleAttributeDefinitionBuilder(ModelKeys.CACHE, ModelType.STRING, true)
                    .setXmlName(Attribute.CACHE.getLocalName())
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                    .build();
    static final SimpleAttributeDefinition HOTROD_WRAPPING =
            new SimpleAttributeDefinitionBuilder(ModelKeys.HOTROD_WRAPPING, ModelType.BOOLEAN, true)
                    .setXmlName(Attribute.HOTROD_WRAPPING.getLocalName())
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                    .setDefaultValue(new ModelNode().set(false))
                    .build();
    static final SimpleAttributeDefinition PROTOCOL_VERSION =
           new SimpleAttributeDefinitionBuilder(ModelKeys.PROTOCOL_VERSION, ModelType.STRING, true)
                   .setXmlName(Attribute.PROTOCOL_VERSION.getLocalName())
                   .setAllowExpression(true)
                   .setValidator(new EnumValidator<>(ProtocolVersion.class, true, true))
                   .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                   .build();
    static final SimpleAttributeDefinition RAW_VALUES =
            new SimpleAttributeDefinitionBuilder(ModelKeys.RAW_VALUES, ModelType.BOOLEAN, true)
                    .setXmlName(Attribute.RAW_VALUES.getLocalName())
                    .setAllowExpression(false)
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                    .setDefaultValue(new ModelNode().set(false))
                    .build();
    static final SimpleAttributeDefinition TCP_NO_DELAY =
            new SimpleAttributeDefinitionBuilder(ModelKeys.TCP_NO_DELAY, ModelType.BOOLEAN, true)
                    .setXmlName(Attribute.TCP_NO_DELAY.getLocalName())
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                    .setDefaultValue(new ModelNode().set(true))
                    .build();
    static final SimpleAttributeDefinition SOCKET_TIMEOUT =
            new SimpleAttributeDefinitionBuilder(ModelKeys.SOCKET_TIMEOUT, ModelType.LONG, true)
                    .setXmlName(Attribute.SOCKET_TIMEOUT.getLocalName())
                    .setMeasurementUnit(MeasurementUnit.MILLISECONDS)
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                    .setDefaultValue(new ModelNode().set(60000))
                    .build();

    static final SimpleAttributeDefinition OUTBOUND_SOCKET_BINDING = new SimpleAttributeDefinitionBuilder(ModelKeys.OUTBOUND_SOCKET_BINDING, ModelType.STRING, true).build();

    static final SimpleAttributeDefinition NAME =
            new SimpleAttributeDefinitionBuilder(BaseStoreConfigurationResource.NAME)
                   .setDefaultValue(new ModelNode().set(ModelKeys.REMOTE_STORE_NAME))
                   .build();

    static final ObjectTypeAttributeDefinition REMOTE_SERVER = ObjectTypeAttributeDefinition.
            Builder.of(ModelKeys.REMOTE_SERVER, OUTBOUND_SOCKET_BINDING).
            setRequired(false).
            setSuffix("remote-server").
            build();

    static final ObjectListAttributeDefinition REMOTE_SERVERS = ObjectListAttributeDefinition.Builder.of(ModelKeys.REMOTE_SERVERS, REMOTE_SERVER).
            setRequired(true).
            build();

    static final AttributeDefinition[] REMOTE_STORE_ATTRIBUTES = {CACHE, HOTROD_WRAPPING, TCP_NO_DELAY, RAW_VALUES, SOCKET_TIMEOUT, REMOTE_SERVERS, PROTOCOL_VERSION};

    public RemoteStoreConfigurationResource(CacheConfigurationResource parent) {
        super(REMOTE_STORE_PATH, ModelKeys.REMOTE_STORE, parent, REMOTE_STORE_ATTRIBUTES);
    }

   @Override
   public void registerChildren(ManagementResourceRegistration resourceRegistration) {
      super.registerChildren(resourceRegistration);
      // child resources
      resourceRegistration.registerSubModel(new AuthenticationResource());
      resourceRegistration.registerSubModel(new EncryptionResource());
   }

}
