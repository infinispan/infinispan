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

import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.AbstractStoreConfiguration;
import org.infinispan.persistence.jdbc.DatabaseType;
import org.infinispan.persistence.jdbc.configuration.TableManipulationConfiguration;
import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.ObjectTypeAttributeDefinition;
import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.SimpleAttributeDefinition;
import org.jboss.as.controller.SimpleAttributeDefinitionBuilder;
import org.jboss.as.controller.operations.validation.EnumValidator;
import org.jboss.as.controller.registry.AttributeAccess;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;

/**
 * Base class for store resources which require common store attributes and JDBC store attributes
 *
 * @author Richard Achmatowicz (c) 2011 Red Hat Inc.
 */
public class BaseJDBCStoreConfigurationResource extends BaseStoreConfigurationResource {

    // attributes
    static final SimpleAttributeDefinition DATA_SOURCE =
            new SimpleAttributeDefinitionBuilder(ModelKeys.DATASOURCE, ModelType.STRING, false)
                    .setXmlName(Attribute.DATASOURCE.getLocalName())
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                    .build();
    static final SimpleAttributeDefinition DIALECT = new SimpleAttributeDefinitionBuilder(ModelKeys.DIALECT, ModelType.STRING, true)
                    .setXmlName(Attribute.DIALECT.getLocalName())
                    .setValidator(new EnumValidator<>(DatabaseType.class, true, true))
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                    .build();

    static final SimpleAttributeDefinition DB_MAJOR_VERSION =
            new SimpleAttributeDefinitionBuilder(ModelKeys.DB_MAJOR_VERSION, ModelType.INT, true)
                    .setXmlName(Attribute.DB_MAJOR_VERSION.getLocalName())
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                    .build();

    static final SimpleAttributeDefinition DB_MINOR_VERSION =
            new SimpleAttributeDefinitionBuilder(ModelKeys.DB_MINOR_VERSION, ModelType.INT, true)
                    .setXmlName(Attribute.DB_MINOR_VERSION.getLocalName())
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                    .build();

    static final AttributeDefinition[] COMMON_JDBC_STORE_ATTRIBUTES = { DATA_SOURCE, DIALECT, DB_MAJOR_VERSION,
                                                                        DB_MINOR_VERSION };

    static final SimpleAttributeDefinition BATCH_SIZE =
            new SimpleAttributeDefinitionBuilder(ModelKeys.BATCH_SIZE, ModelType.INT, true)
                    .setXmlName(Attribute.BATCH_SIZE.getLocalName())
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                    .setDeprecated(Namespace.INFINISPAN_SERVER_9_1.getVersion())
                    .setDefaultValue(new ModelNode().set(AbstractStoreConfiguration.MAX_BATCH_SIZE.getDefaultValue()))
                    .build();
    static final SimpleAttributeDefinition FETCH_SIZE =
            new SimpleAttributeDefinitionBuilder(ModelKeys.FETCH_SIZE, ModelType.INT, true)
                    .setXmlName(Attribute.FETCH_SIZE.getLocalName())
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                    .setDefaultValue(new ModelNode().set(TableManipulationConfiguration.FETCH_SIZE.getDefaultValue()))
                    .build();
    static final SimpleAttributeDefinition PREFIX =
            new SimpleAttributeDefinitionBuilder(ModelKeys.PREFIX, ModelType.STRING, true)
                    .setXmlName(Attribute.PREFIX.getLocalName())
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                    .build();
    static final SimpleAttributeDefinition CREATE_ON_START =
            new SimpleAttributeDefinitionBuilder(ModelKeys.CREATE_ON_START, ModelType.BOOLEAN, true)
                    .setXmlName(Attribute.CREATE_ON_START.getLocalName())
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                    .setDefaultValue(new ModelNode().set(TableManipulationConfiguration.CREATE_ON_START.getDefaultValue()))
                    .build();
    static final SimpleAttributeDefinition DROP_ON_EXIT =
            new SimpleAttributeDefinitionBuilder(ModelKeys.DROP_ON_EXIT, ModelType.BOOLEAN, true)
                    .setXmlName(Attribute.DROP_ON_EXIT.getLocalName())
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                    .setDefaultValue(new ModelNode().set(TableManipulationConfiguration.DROP_ON_EXIT.getDefaultValue()))
                    .build();

    static final SimpleAttributeDefinition COLUMN_NAME =
            new SimpleAttributeDefinitionBuilder("name", ModelType.STRING, true)
                    .setXmlName("name")
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                    .setDefaultValue(new ModelNode().set("name"))
                    .build();
    static final SimpleAttributeDefinition COLUMN_TYPE =
            new SimpleAttributeDefinitionBuilder("type", ModelType.STRING, true)
                    .setXmlName("type")
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                    .setDefaultValue(new ModelNode().set("type"))
                    .build();

    static final SimpleAttributeDefinition STRING_KEYED_TABLE_PREFIX = new SimpleAttributeDefinitionBuilder("stringKeyedTablePrefix", ModelType.STRING)
          .setDefaultValue(new ModelNode().set("ispn_entry"))
          .build();

    static final ObjectTypeAttributeDefinition ID_COLUMN = ObjectTypeAttributeDefinition.
            Builder.of("id-column", COLUMN_NAME, COLUMN_TYPE).
            setRequired(false).
            setSuffix("column").
            build();

    static final ObjectTypeAttributeDefinition DATA_COLUMN = ObjectTypeAttributeDefinition.
            Builder.of("data-column", COLUMN_NAME, COLUMN_TYPE).
            setRequired(false).
            setSuffix("column").
            build();

    static final ObjectTypeAttributeDefinition TIMESTAMP_COLUMN = ObjectTypeAttributeDefinition.
            Builder.of("timestamp-column", COLUMN_NAME, COLUMN_TYPE).
            setRequired(false).
            setSuffix("column").
            build();

    static final ObjectTypeAttributeDefinition STRING_KEYED_TABLE = ObjectTypeAttributeDefinition.
            Builder.of(ModelKeys.STRING_KEYED_TABLE, PREFIX, BATCH_SIZE, FETCH_SIZE, CREATE_ON_START, DROP_ON_EXIT, ID_COLUMN, DATA_COLUMN, TIMESTAMP_COLUMN).
            setRequired(false).
            setSuffix("table").
            build();

    public BaseJDBCStoreConfigurationResource(PathElement path, String resourceKey, CacheConfigurationResource parent, AttributeDefinition[] attributes) {
        super(path, resourceKey, parent, Util.arrayConcat(COMMON_JDBC_STORE_ATTRIBUTES, attributes));
    }

}
