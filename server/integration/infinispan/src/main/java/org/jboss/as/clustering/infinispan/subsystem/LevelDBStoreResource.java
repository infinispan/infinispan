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

import org.jboss.as.controller.*;
import org.jboss.as.controller.registry.AttributeAccess;
import org.jboss.as.controller.registry.ManagementResourceRegistration;
import org.jboss.as.controller.registry.OperationEntry;
import org.jboss.as.controller.services.path.ResolvePathHandler;
import org.jboss.as.server.ServerEnvironment;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;

import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.ADD;

/**
 * Resource description for the addressable resource /subsystem=infinispan/cache-container=X/cache=Y/store=STORE
 *
 * @author Galder Zamarreño
 */
public class LevelDBStoreResource extends BaseStoreResource {

    public static final PathElement LEVELDB_STORE_PATH = PathElement.pathElement(ModelKeys.LEVELDB_STORE);

    // attributes
    static final SimpleAttributeDefinition PATH =
            new SimpleAttributeDefinitionBuilder(ModelKeys.PATH, ModelType.STRING, true)
                    .setXmlName(Attribute.PATH.getLocalName())
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_ALL_SERVICES)
                    .build();

    static final SimpleAttributeDefinition RELATIVE_TO =
            new SimpleAttributeDefinitionBuilder(ModelKeys.RELATIVE_TO, ModelType.STRING, true)
                    .setXmlName(Attribute.RELATIVE_TO.getLocalName())
                    .setAllowExpression(false)
                    .setFlags(AttributeAccess.Flag.RESTART_ALL_SERVICES)
                    .setDefaultValue(new ModelNode().set(ServerEnvironment.SERVER_DATA_DIR))
                    .build();

    static final SimpleAttributeDefinition BLOCK_SIZE =
            new SimpleAttributeDefinitionBuilder(ModelKeys.BLOCK_SIZE, ModelType.INT, true)
                    .setXmlName(Attribute.BLOCK_SIZE.getLocalName())
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_ALL_SERVICES)
                    .setDefaultValue(new ModelNode().set(0))
                    .build();

    static final SimpleAttributeDefinition CACHE_SIZE =
            new SimpleAttributeDefinitionBuilder(ModelKeys.CACHE_SIZE, ModelType.LONG, true)
                    .setXmlName(Attribute.CACHE_SIZE.getLocalName())
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_ALL_SERVICES)
                    .setDefaultValue(new ModelNode().set(0))
                    .build();

    static final SimpleAttributeDefinition CLEAR_THRESHOLD =
            new SimpleAttributeDefinitionBuilder(ModelKeys.CLEAR_THRESHOLD, ModelType.INT, true)
                    .setXmlName(Attribute.CLEAR_THRESHOLD.getLocalName())
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_ALL_SERVICES)
                    .setDefaultValue(new ModelNode().set(10000))
                    .build();

    static final AttributeDefinition[] LEVELDB_STORE_ATTRIBUTES = {PATH, BLOCK_SIZE, CACHE_SIZE, CLEAR_THRESHOLD};

    static final SimpleAttributeDefinition NAME =
            new SimpleAttributeDefinitionBuilder(BaseStoreResource.NAME)
                    .setDefaultValue(new ModelNode().set(ModelKeys.LEVELDB_STORE_NAME))
                    .build();

    // operations
    private static final OperationDefinition LEVELDB_STORE_ADD_DEFINITION = new SimpleOperationDefinitionBuilder(ADD, InfinispanExtension.getResourceDescriptionResolver(ModelKeys.LEVELDB_STORE))
        .setParameters(COMMON_STORE_PARAMETERS)
        .addParameter(PATH)
        .addParameter(BLOCK_SIZE)
        .addParameter(CACHE_SIZE)
        .addParameter(CLEAR_THRESHOLD)
        .setAttributeResolver(InfinispanExtension.getResourceDescriptionResolver(ModelKeys.LEVELDB_STORE))
        .build();

    private final ResolvePathHandler resolvePathHandler;

    public LevelDBStoreResource(final ResolvePathHandler resolvePathHandler) {
        super(LEVELDB_STORE_PATH,
                InfinispanExtension.getResourceDescriptionResolver(ModelKeys.LEVELDB_STORE),
                CacheConfigOperationHandlers.LEVELDB_STORE_ADD,
                ReloadRequiredRemoveStepHandler.INSTANCE);
        this.resolvePathHandler = resolvePathHandler;
    }

    @Override
    public void registerAttributes(ManagementResourceRegistration resourceRegistration) {
        super.registerAttributes(resourceRegistration);

        // check that we don't need a special handler here?
        final OperationStepHandler writeHandler = new ReloadRequiredWriteAttributeHandler(LEVELDB_STORE_ATTRIBUTES);
        for (AttributeDefinition attr : LEVELDB_STORE_ATTRIBUTES) {
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
        registration.registerOperationHandler(LEVELDB_STORE_ADD_DEFINITION, handler);
        if (resolvePathHandler != null) {
            registration.registerOperationHandler(resolvePathHandler.getOperationDefinition(), resolvePathHandler);
        }
    }

    @Override
    public void registerChildren(ManagementResourceRegistration resourceRegistration) {
        super.registerChildren(resourceRegistration);
        // child resources
        resourceRegistration.registerSubModel(new LevelDBExpirationResource());
        resourceRegistration.registerSubModel(new LevelDBCompressionResource());
        resourceRegistration.registerSubModel(new LevelDBImplementationResource());
    }

}
