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
import org.jboss.as.controller.registry.ManagementResourceRegistration;
import org.jboss.as.controller.registry.OperationEntry;
import org.jboss.dmr.ModelNode;

/**
 * Resource description for the addressable resource
 *
 * /subsystem=infinispan/cache-container=X/cache=Y/binary-keyed-jdbc-store=BINARY_KEYED_JDBC_STORE
 *
 * @author Richard Achmatowicz (c) 2011 Red Hat Inc.
 * @author Tristan Tarrant
 */
public class BinaryKeyedJDBCStoreResource extends BaseJDBCStoreResource {

    public static final PathElement BINARY_KEYED_JDBC_STORE_PATH = PathElement.pathElement(ModelKeys.BINARY_KEYED_JDBC_STORE);

    // attributes
    static final AttributeDefinition[] BINARY_KEYED_JDBC_STORE_ATTRIBUTES = {BINARY_KEYED_TABLE};

    static final SimpleAttributeDefinition NAME =
        new SimpleAttributeDefinitionBuilder(BaseStoreResource.NAME)
            .setDefaultValue(new ModelNode().set(ModelKeys.BINARY_KEYED_TABLE_NAME))
            .build();

    // operations
    private static final OperationDefinition BINARY_KEYED_JDBC_STORE_ADD_DEFINITION = new SimpleOperationDefinitionBuilder(ADD, new InfinispanResourceDescriptionResolver(ModelKeys.BINARY_KEYED_JDBC_STORE))
        .setParameters(COMMON_STORE_PARAMETERS)
        .addParameter(DATA_SOURCE)
        .addParameter(DIALECT)
        .addParameter(BINARY_KEYED_TABLE)
        .build();

    public BinaryKeyedJDBCStoreResource(CacheResource cacheResource) {
        super(BINARY_KEYED_JDBC_STORE_PATH, ModelKeys.BINARY_KEYED_JDBC_STORE, cacheResource, BINARY_KEYED_JDBC_STORE_ATTRIBUTES);
    }

    // override the add operation to provide a custom definition (for the optional PROPERTIES parameter to add())
    @Override
    protected void registerAddOperation(final ManagementResourceRegistration registration, final OperationStepHandler handler, OperationEntry.Flag... flags) {
        registration.registerOperationHandler(BINARY_KEYED_JDBC_STORE_ADD_DEFINITION, handler);
    }

}
