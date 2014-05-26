/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
package org.infinispan.server.endpoint.subsystem;

import org.infinispan.rest.configuration.ExtendedHeaders;
import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.OperationStepHandler;
import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.ReloadRequiredWriteAttributeHandler;
import org.jboss.as.controller.SimpleAttributeDefinition;
import org.jboss.as.controller.SimpleAttributeDefinitionBuilder;
import org.jboss.as.controller.operations.validation.EnumValidator;
import org.jboss.as.controller.registry.ManagementResourceRegistration;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;

public class RestConnectorResource extends CommonConnectorResource {
   public static final PathElement REST_CONNECTOR_PATH = PathElement.pathElement(ModelKeys.REST_CONNECTOR);

   static final SimpleAttributeDefinition AUTH_METHOD =
         new SimpleAttributeDefinitionBuilder(ModelKeys.AUTH_METHOD, ModelType.STRING, true)
                 .setAllowExpression(true)
                 .setXmlName(ModelKeys.AUTH_METHOD)
                 .setValidator(new EnumValidator<AuthMethod>(AuthMethod.class, true, false))
                 .setRestartAllServices()
                 .build();

   static final SimpleAttributeDefinition CONTEXT_PATH =
         new SimpleAttributeDefinitionBuilder(ModelKeys.CONTEXT_PATH, ModelType.STRING, true)
                 .setAllowExpression(true)
                 .setXmlName(ModelKeys.CONTEXT_PATH)
                 .setRestartAllServices()
                 .build();

   static final SimpleAttributeDefinition EXTENDED_HEADERS =
         new SimpleAttributeDefinitionBuilder(ModelKeys.EXTENDED_HEADERS, ModelType.STRING, true)
                 .setAllowExpression(true)
                 .setXmlName(ModelKeys.EXTENDED_HEADERS)
                 .setValidator(new EnumValidator<ExtendedHeaders>(ExtendedHeaders.class, true, false))
                 .setDefaultValue(new ModelNode().set(ExtendedHeaders.ON_DEMAND.name()))
                 .setRestartAllServices()
                 .build();

   static final SimpleAttributeDefinition SECURITY_DOMAIN =
         new SimpleAttributeDefinitionBuilder(ModelKeys.SECURITY_DOMAIN, ModelType.STRING, true)
                 .setAllowExpression(true)
                 .setXmlName(ModelKeys.SECURITY_DOMAIN)
                 .setRestartAllServices()
                 .build();

   static final SimpleAttributeDefinition SECURITY_MODE =
         new SimpleAttributeDefinitionBuilder(ModelKeys.SECURITY_MODE, ModelType.STRING, true)
                 .setAllowExpression(true)
                 .setXmlName(ModelKeys.SECURITY_MODE)
                 .setValidator(new EnumValidator<SecurityMode>(SecurityMode.class, true, false))
                 .setDefaultValue(new ModelNode().set(SecurityMode.READ_WRITE.name()))
                 .setRestartAllServices()
                 .build();

   static final SimpleAttributeDefinition VIRTUAL_SERVER =
         new SimpleAttributeDefinitionBuilder(ModelKeys.VIRTUAL_SERVER, ModelType.STRING, true)
                 .setAllowExpression(true)
                 .setXmlName(ModelKeys.VIRTUAL_SERVER)
                 .setRestartAllServices()
                 .build();

   static final SimpleAttributeDefinition[] REST_ATTRIBUTES = { AUTH_METHOD, CONTEXT_PATH, EXTENDED_HEADERS, SECURITY_DOMAIN, SECURITY_MODE, VIRTUAL_SERVER };

   public RestConnectorResource(boolean isRuntimeRegistration) {
      super(REST_CONNECTOR_PATH, EndpointExtension.getResourceDescriptionResolver(ModelKeys.REST_CONNECTOR), RestSubsystemAdd.INSTANCE, RestSubsystemRemove.INSTANCE, isRuntimeRegistration);
   }

   @Override
   public void registerAttributes(ManagementResourceRegistration resourceRegistration) {
      super.registerAttributes(resourceRegistration);

      final OperationStepHandler writeHandler = new ReloadRequiredWriteAttributeHandler(REST_ATTRIBUTES);
      for (AttributeDefinition attr : REST_ATTRIBUTES) {
         resourceRegistration.registerReadWriteAttribute(attr, null, writeHandler);
      }
   }


}
