/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2011, Red Hat Middleware LLC, and individual contributors
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
package org.infinispan.server.endpoint.subsystem;

import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.OperationStepHandler;
import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.ReloadRequiredAddStepHandler;
import org.jboss.as.controller.ReloadRequiredRemoveStepHandler;
import org.jboss.as.controller.ReloadRequiredWriteAttributeHandler;
import org.jboss.as.controller.SimpleAttributeDefinition;
import org.jboss.as.controller.SimpleAttributeDefinitionBuilder;
import org.jboss.as.controller.SimpleResourceDefinition;
import org.jboss.as.controller.registry.ManagementResourceRegistration;
import org.jboss.dmr.ModelType;

/**
 * PrefixResource.
 *
 * @author Sebastian Łaskawiec
 * @since 9.0
 */
public class PrefixResource extends SimpleResourceDefinition {
   public static final PathElement PREFIX_PATH = PathElement.pathElement(ModelKeys.PREFIX);

   static final SimpleAttributeDefinition PATH =
         new SimpleAttributeDefinitionBuilder(ModelKeys.PATH, ModelType.STRING, true)
            .setAllowExpression(true)
            .setXmlName(ModelKeys.PATH)
            .setRestartAllServices()
            .build();

   static final SimpleAttributeDefinition[] PREFIX_ATTRIBUTES = { PATH };

   PrefixResource() {
      super(PREFIX_PATH, EndpointExtension.getResourceDescriptionResolver(ModelKeys.PREFIX),
            new ReloadRequiredAddStepHandler(PREFIX_ATTRIBUTES), ReloadRequiredRemoveStepHandler.INSTANCE);
   }

   @Override
   public void registerAttributes(ManagementResourceRegistration resourceRegistration) {
      final OperationStepHandler writeHandler = new ReloadRequiredWriteAttributeHandler(PREFIX_ATTRIBUTES);
      for (AttributeDefinition attr : PREFIX_ATTRIBUTES) {
          resourceRegistration.registerReadWriteAttribute(attr, null, writeHandler);
      }
   }
}
