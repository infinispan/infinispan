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

import org.infinispan.globalstate.ConfigurationStorage;
import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.ObjectTypeAttributeDefinition;
import org.jboss.as.controller.OperationStepHandler;
import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.ReloadRequiredAddStepHandler;
import org.jboss.as.controller.ReloadRequiredRemoveStepHandler;
import org.jboss.as.controller.ReloadRequiredWriteAttributeHandler;
import org.jboss.as.controller.SimpleAttributeDefinition;
import org.jboss.as.controller.SimpleAttributeDefinitionBuilder;
import org.jboss.as.controller.SimpleResourceDefinition;
import org.jboss.as.controller.operations.validation.EnumValidator;
import org.jboss.as.controller.registry.AttributeAccess;
import org.jboss.as.controller.registry.ManagementResourceRegistration;
import org.jboss.as.server.ServerEnvironment;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;

/**
 * Resource description for the addressable resource
 * /subsystem=infinispan/cache-container=X/global-state=GLOBAL_STATE
 *
 * @author Tristan Tarrant
 */
public class GlobalStateResource extends SimpleResourceDefinition {

    public static final PathElement GLOBAL_STATE_PATH = PathElement.pathElement(ModelKeys.GLOBAL_STATE,
            ModelKeys.GLOBAL_STATE_NAME);

    // attributes

    static final SimpleAttributeDefinition PATH = new SimpleAttributeDefinitionBuilder(ModelKeys.PATH, ModelType.STRING,
            false).setXmlName(Attribute.PATH.getLocalName()).setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES).build();

    static final SimpleAttributeDefinition PERSISTENT_RELATIVE_TO = new SimpleAttributeDefinitionBuilder(ModelKeys.RELATIVE_TO,
            ModelType.STRING, true).setXmlName(Attribute.RELATIVE_TO.getLocalName()).setAllowExpression(false)
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                    .setDefaultValue(new ModelNode().set(ServerEnvironment.SERVER_DATA_DIR)).build();

    static final SimpleAttributeDefinition SHARED_PERSISTENT_RELATIVE_TO = new SimpleAttributeDefinitionBuilder(ModelKeys.RELATIVE_TO,
          ModelType.STRING, true).setXmlName(Attribute.RELATIVE_TO.getLocalName()).setAllowExpression(false)
          .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
          .build();

    static final SimpleAttributeDefinition TEMPORARY_RELATIVE_TO = new SimpleAttributeDefinitionBuilder(ModelKeys.RELATIVE_TO,
            ModelType.STRING, true).setXmlName(Attribute.RELATIVE_TO.getLocalName()).setAllowExpression(false)
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                    .setDefaultValue(new ModelNode().set(ServerEnvironment.SERVER_TEMP_DIR)).build();

    static final ObjectTypeAttributeDefinition PERSISTENT_LOCATION_PATH =
            ObjectTypeAttributeDefinition.Builder.of(ModelKeys.PERSISTENT_LOCATION, PATH, PERSISTENT_RELATIVE_TO)
                .setRequired(false)
                .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                .setXmlName(ModelKeys.PERSISTENT_LOCATION)
                .build();

    static final ObjectTypeAttributeDefinition SHARED_PERSISTENT_LOCATION_PATH =
          ObjectTypeAttributeDefinition.Builder.of(ModelKeys.SHARED_PERSISTENT_LOCATION, PATH, SHARED_PERSISTENT_RELATIVE_TO)
                .setRequired(false)
                .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                .setXmlName(ModelKeys.PERSISTENT_LOCATION)
                .build();

    static final ObjectTypeAttributeDefinition TEMPORARY_STATE_PATH =
            ObjectTypeAttributeDefinition.Builder.of(ModelKeys.TEMPORARY_LOCATION, PATH, TEMPORARY_RELATIVE_TO)
                .setRequired(false)
                .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                .setXmlName(ModelKeys.TEMPORARY_LOCATION)
                .build();

    static final SimpleAttributeDefinition CONFIGURATION_STORAGE =
          new SimpleAttributeDefinitionBuilder(ModelKeys.CONFIGURATION_STORAGE, ModelType.STRING, true)
                .setXmlName(Attribute.CONFIGURATION_STORAGE.getLocalName())
                .setAllowExpression(true)
                .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                .setValidator(new EnumValidator<>(ConfigurationStorage.class, false, true))
                .setDefaultValue(new ModelNode().set(ConfigurationStorage.OVERLAY.toString()))
                .build();

    static final SimpleAttributeDefinition CONFIGURATION_STORAGE_CLASS =
          new SimpleAttributeDefinitionBuilder(ModelKeys.CONFIGURATION_STORAGE_CLASS, ModelType.STRING, true)
                .setXmlName(Attribute.CONFIGURATION_STORAGE_CLASS.getLocalName())
                .setAllowExpression(true)
                .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                .build();

    static final AttributeDefinition[] ATTRIBUTES = {
          PERSISTENT_LOCATION_PATH, SHARED_PERSISTENT_LOCATION_PATH, TEMPORARY_STATE_PATH,
          CONFIGURATION_STORAGE,  CONFIGURATION_STORAGE_CLASS
    };

    GlobalStateResource() {
        super(GLOBAL_STATE_PATH,
                new InfinispanResourceDescriptionResolver(ModelKeys.CACHE_CONTAINER, ModelKeys.GLOBAL_STATE),
                new ReloadRequiredAddStepHandler(ATTRIBUTES), ReloadRequiredRemoveStepHandler.INSTANCE);
    }

    @Override
    public void registerAttributes(ManagementResourceRegistration registration) {
        final OperationStepHandler writeHandler = new ReloadRequiredWriteAttributeHandler(ATTRIBUTES);
        for (AttributeDefinition attribute : ATTRIBUTES) {
            registration.registerReadWriteAttribute(attribute, null, writeHandler);
        }
    }
}
