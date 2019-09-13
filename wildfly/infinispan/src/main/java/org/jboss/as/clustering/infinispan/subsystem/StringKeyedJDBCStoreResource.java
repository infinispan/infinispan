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

import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.SimpleAttributeDefinition;
import org.jboss.as.controller.SimpleAttributeDefinitionBuilder;
import org.jboss.as.controller.registry.ManagementResourceRegistration;
import org.jboss.dmr.ModelNode;

/**
 * Resource description for the addressable resource
 *
 * /subsystem=infinispan/cache-container=X/cache=Y/string-keyed-jdbc-store=STRING_KEYED_JDBC_STORE
 *
 * @author Richard Achmatowicz (c) 2011 Red Hat Inc.
 * @author Tristan Tarrant
 */
public class StringKeyedJDBCStoreResource extends BaseJDBCStoreConfigurationResource {

    public static final PathElement PATH = PathElement.pathElement(ModelKeys.STRING_KEYED_JDBC_STORE);

    // attributes
    static final AttributeDefinition[] ATTRIBUTES = {STRING_KEYED_TABLE};

   static final SimpleAttributeDefinition NAME =
         new SimpleAttributeDefinitionBuilder(BaseStoreConfigurationResource.NAME)
               .setDefaultValue(new ModelNode().set(ModelKeys.STRING_KEYED_TABLE_NAME))
               .build();

    public StringKeyedJDBCStoreResource(CacheConfigurationResource parent, ManagementResourceRegistration containerReg) {
        super(PATH, ModelKeys.STRING_KEYED_JDBC_STORE, parent, containerReg, ATTRIBUTES);
    }
}
