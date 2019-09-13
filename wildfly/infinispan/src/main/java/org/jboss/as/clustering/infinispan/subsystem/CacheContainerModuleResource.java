/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2017, Red Hat, Inc., and individual contributors
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
import org.jboss.as.controller.OperationStepHandler;
import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.ReloadRequiredAddStepHandler;
import org.jboss.as.controller.ReloadRequiredRemoveStepHandler;
import org.jboss.as.controller.ReloadRequiredWriteAttributeHandler;
import org.jboss.as.controller.SimpleAttributeDefinition;
import org.jboss.as.controller.SimpleAttributeDefinitionBuilder;
import org.jboss.as.controller.SimpleResourceDefinition;
import org.jboss.as.controller.registry.AttributeAccess;
import org.jboss.as.controller.registry.ManagementResourceRegistration;
import org.jboss.dmr.ModelType;

/**
 * @author anistor@redhat.com
 * @since 9.2
 */
public class CacheContainerModuleResource extends SimpleResourceDefinition {

   static final SimpleAttributeDefinition NAME = new SimpleAttributeDefinitionBuilder(ModelKeys.NAME, ModelType.STRING, false)
         .setXmlName(Attribute.NAME.getLocalName())
         .setAllowExpression(true)
         .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
         .build();

   static final SimpleAttributeDefinition SLOT = new SimpleAttributeDefinitionBuilder(ModelKeys.SLOT, ModelType.STRING, true)
         .setXmlName(Attribute.SLOT.getLocalName())
         .setAllowExpression(true)
         .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
         .build();

   private static final AttributeDefinition[] ATTRIBUTES = new AttributeDefinition[]{NAME, SLOT};

   CacheContainerModuleResource() {
      super(PathElement.pathElement(ModelKeys.MODULE),
            new InfinispanResourceDescriptionResolver(ModelKeys.CACHE_CONTAINER, ModelKeys.MODULES, ModelKeys.MODULE),
            new ReloadRequiredAddStepHandler(ATTRIBUTES),
            ReloadRequiredRemoveStepHandler.INSTANCE);
   }

   @Override
   public void registerAttributes(ManagementResourceRegistration registration) {
      OperationStepHandler writeHandler = new ReloadRequiredWriteAttributeHandler(ATTRIBUTES);
      for (AttributeDefinition attribute : ATTRIBUTES) {
         registration.registerReadWriteAttribute(attribute, null, writeHandler);
      }
   }
}
