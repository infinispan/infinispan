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
import org.jboss.as.controller.OperationDefinition;
import org.jboss.as.controller.OperationStepHandler;
import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.SimpleAttributeDefinition;
import org.jboss.as.controller.SimpleAttributeDefinitionBuilder;
import org.jboss.as.controller.SimpleOperationDefinitionBuilder;
import org.jboss.as.controller.registry.AttributeAccess;
import org.jboss.as.controller.registry.ManagementResourceRegistration;
import org.jboss.as.controller.registry.OperationEntry;
import org.jboss.as.controller.services.path.ResolvePathHandler;
import org.jboss.as.server.ServerEnvironment;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;

/**
 * Resource description for the addressable resource /subsystem=infinispan/cache-container=X/cache=Y/store=STORE
 *
 * @author Richard Achmatowicz (c) 2011 Red Hat Inc.
 */
public class FileStoreResource extends BaseStoreResource {

    public static final PathElement FILE_STORE_PATH = PathElement.pathElement(ModelKeys.FILE_STORE);

    // attributes

    static final SimpleAttributeDefinition MAX_ENTRIES =
            new SimpleAttributeDefinitionBuilder(ModelKeys.MAX_ENTRIES, ModelType.INT, true)
                    .setXmlName(Attribute.MAX_ENTRIES.getLocalName())
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                    .build();

    static final SimpleAttributeDefinition PATH =
            new SimpleAttributeDefinitionBuilder(ModelKeys.PATH, ModelType.STRING, true)
                    .setXmlName(Attribute.PATH.getLocalName())
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                    .build();

    static final SimpleAttributeDefinition RELATIVE_TO =
            new SimpleAttributeDefinitionBuilder(ModelKeys.RELATIVE_TO, ModelType.STRING, true)
                    .setXmlName(Attribute.RELATIVE_TO.getLocalName())
                    .setAllowExpression(false)
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                    .setDefaultValue(new ModelNode().set(ServerEnvironment.SERVER_DATA_DIR))
                    .build();

    static final AttributeDefinition[] FILE_STORE_ATTRIBUTES = {MAX_ENTRIES, RELATIVE_TO, PATH};

    static final SimpleAttributeDefinition NAME =
            new SimpleAttributeDefinitionBuilder(BaseStoreResource.NAME)
                    .setDefaultValue(new ModelNode().set(ModelKeys.FILE_STORE_NAME))
                    .build();

    // operations
    private static final OperationDefinition FILE_STORE_ADD_DEFINITION = new SimpleOperationDefinitionBuilder(ADD, new InfinispanResourceDescriptionResolver(ModelKeys.FILE_STORE))
        .setParameters(COMMON_STORE_PARAMETERS)
        .addParameter(MAX_ENTRIES)
        .addParameter(RELATIVE_TO)
        .addParameter(PATH)
        .setAttributeResolver(new InfinispanResourceDescriptionResolver(ModelKeys.FILE_STORE))
        .build();

    private final ResolvePathHandler resolvePathHandler;

    public FileStoreResource(CacheResource cacheResource, ResolvePathHandler resolvePathHandler) {
        super(FILE_STORE_PATH, ModelKeys.FILE_STORE, cacheResource, FILE_STORE_ATTRIBUTES);
        this.resolvePathHandler = resolvePathHandler;
    }

    @Override
    public void registerOperations(ManagementResourceRegistration resourceRegistration) {
        super.registerOperations(resourceRegistration);
    }

    // override the add operation to provide a custom definition (for the optional PROPERTIES parameter to add())
    @Override
    protected void registerAddOperation(final ManagementResourceRegistration registration, final OperationStepHandler handler, OperationEntry.Flag... flags) {
        registration.registerOperationHandler(FILE_STORE_ADD_DEFINITION, handler);
        if (resolvePathHandler != null) {
            registration.registerOperationHandler(resolvePathHandler.getOperationDefinition(), resolvePathHandler);
        }
    }

}
