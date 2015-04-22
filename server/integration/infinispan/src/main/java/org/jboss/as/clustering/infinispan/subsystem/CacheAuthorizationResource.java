/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2013, Red Hat, Inc., and individual contributors
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
import org.jboss.as.controller.StringListAttributeDefinition;
import org.jboss.as.controller.registry.AttributeAccess;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;

/**
 * CacheAuthorizationResource.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class CacheAuthorizationResource extends CacheChildResource {

    static final SimpleAttributeDefinition ENABLED = new SimpleAttributeDefinitionBuilder(ModelKeys.ENABLED, ModelType.BOOLEAN, true)
            .setXmlName(Attribute.ENABLED.getLocalName())
            .setAllowExpression(true)
            .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
            .setDefaultValue(new ModelNode().set(true))
            .build()
    ;

    static final StringListAttributeDefinition ROLES = new StringListAttributeDefinition.Builder(ModelKeys.ROLES)
            .setXmlName(Attribute.ROLES.getLocalName())
            .setAllowExpression(true)
            .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
            .setAllowNull(true)
            .build()
    ;

    static final AttributeDefinition[] ATTRIBUTES = new AttributeDefinition[] { ENABLED, ROLES };

    CacheAuthorizationResource(CacheResource cacheResource) {
        super(PathElement.pathElement(ModelKeys.AUTHORIZATION), String.format("%s.%s.%s", ModelKeys.CACHE, ModelKeys.SECURITY, ModelKeys.AUTHORIZATION), cacheResource, ATTRIBUTES);
    }

}
