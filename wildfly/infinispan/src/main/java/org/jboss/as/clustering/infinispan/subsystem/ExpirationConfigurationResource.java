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

import org.infinispan.configuration.cache.ExpirationConfiguration;
import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.SimpleAttributeDefinition;
import org.jboss.as.controller.SimpleAttributeDefinitionBuilder;
import org.jboss.as.controller.client.helpers.MeasurementUnit;
import org.jboss.as.controller.registry.AttributeAccess;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;

/**
 * Resource description for the addressable resource /subsystem=infinispan/cache-container=X/cache=Y/expiration=EXPIRATION
 *
 * @author Richard Achmatowicz (c) 2011 Red Hat Inc.
 * @author Tristan Tarrant
 */
public class ExpirationConfigurationResource extends CacheConfigurationChildResource {

    public static final PathElement PATH = PathElement.pathElement(ModelKeys.EXPIRATION, ModelKeys.EXPIRATION_NAME);

    // attributes
    static final SimpleAttributeDefinition INTERVAL =
            new SimpleAttributeDefinitionBuilder(ModelKeys.INTERVAL, ModelType.LONG, true)
                    .setXmlName(Attribute.INTERVAL.getLocalName())
                    .setMeasurementUnit(MeasurementUnit.MILLISECONDS)
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                    .setDefaultValue(new ModelNode().set(ExpirationConfiguration.WAKEUP_INTERVAL.getDefaultValue()))
                    .build();

    static final SimpleAttributeDefinition LIFESPAN =
            new SimpleAttributeDefinitionBuilder(ModelKeys.LIFESPAN, ModelType.LONG, true)
                    .setXmlName(Attribute.LIFESPAN.getLocalName())
                    .setMeasurementUnit(MeasurementUnit.MILLISECONDS)
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                    .setDefaultValue(new ModelNode().set(ExpirationConfiguration.LIFESPAN.getDefaultValue()))
                    .build();

    static final SimpleAttributeDefinition MAX_IDLE =
            new SimpleAttributeDefinitionBuilder(ModelKeys.MAX_IDLE, ModelType.LONG, true)
                    .setXmlName(Attribute.MAX_IDLE.getLocalName())
                    .setMeasurementUnit(MeasurementUnit.MILLISECONDS)
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                    .setDefaultValue(new ModelNode().set(ExpirationConfiguration.MAX_IDLE.getDefaultValue()))
                    .build();

    static final AttributeDefinition[] ATTRIBUTES = {MAX_IDLE, LIFESPAN, INTERVAL};

    public ExpirationConfigurationResource(CacheConfigurationResource parent) {
        super(PATH, ModelKeys.EXPIRATION, parent, ATTRIBUTES);
    }
}
