/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2015, Red Hat, Inc., and individual contributors
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

package org.infinispan.server.commons.controller;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.descriptions.ResourceDescriptionResolver;

/**
 * Describes the properties of resource used by {@link AddStepHandler}.
 * Supports supplying attributes and capabilities via enumerations.
 * Also supports defining extra parameters that are not actually attributes of the target resource.
 * @author Paul Ferraro
 */
public class ResourceDescriptor implements AddStepHandlerDescriptor {

    private final ResourceDescriptionResolver resolver;
    private final List<AttributeDefinition> attributes = new LinkedList<>();
    private final List<AttributeDefinition> parameters = new LinkedList<>();

    public ResourceDescriptor(ResourceDescriptionResolver resolver) {
        this.resolver = resolver;
    }

    @Override
    public ResourceDescriptionResolver getDescriptionResolver() {
        return this.resolver;
    }

    @Override
    public Collection<AttributeDefinition> getAttributes() {
        return this.attributes;
    }

    @Override
    public Collection<AttributeDefinition> getExtraParameters() {
        return this.parameters;
    }

    public ResourceDescriptor addAttributes(AttributeDefinition... attributes) {
       return this.addAttributes(Arrays.asList(attributes));
    }

   public ResourceDescriptor addAttributes(Iterable<? extends AttributeDefinition> attributes) {
       for (AttributeDefinition attribute : attributes) {
           this.attributes.add(attribute);
       }
       return this;
    }

    public ResourceDescriptor addExtraParameters(AttributeDefinition... parameters) {
        return this.addExtraParameters(Arrays.asList(parameters));
    }

    public ResourceDescriptor addExtraParameters(Collection<? extends AttributeDefinition> parameters) {
        this.parameters.addAll(parameters);
        return this;
    }
}
