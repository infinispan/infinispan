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
import org.infinispan.rest.configuration.RestServerConfigurationBuilder;
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

   static final SimpleAttributeDefinition NAME =
           new SimpleAttributeDefinitionBuilder(ModelKeys.NAME, ModelType.STRING, true)
                   .setAllowExpression(true)
                   .setXmlName(ModelKeys.NAME)
                   .setRestartAllServices()
                   .build();

   static final SimpleAttributeDefinition SOCKET_BINDING =
      new SimpleAttributeDefinitionBuilder(ModelKeys.SOCKET_BINDING, ModelType.STRING, true)
         .setAllowExpression(true)
         .setXmlName(ModelKeys.SOCKET_BINDING)
         .setRestartAllServices()
         .build();

   static final SimpleAttributeDefinition CONTEXT_PATH =
         new SimpleAttributeDefinitionBuilder(ModelKeys.CONTEXT_PATH, ModelType.STRING, true)
               .setAllowExpression(true)
               .setXmlName(ModelKeys.CONTEXT_PATH)
               .setRestartAllServices()
               .setDefaultValue(new ModelNode().set(RestServerConfigurationBuilder.DEFAULT_CONTEXT_PATH))
               .build();

   static final SimpleAttributeDefinition EXTENDED_HEADERS =
         new SimpleAttributeDefinitionBuilder(ModelKeys.EXTENDED_HEADERS, ModelType.STRING, true)
                 .setAllowExpression(true)
                 .setXmlName(ModelKeys.EXTENDED_HEADERS)
                 .setValidator(new EnumValidator<>(ExtendedHeaders.class, true, false))
                 .setDefaultValue(new ModelNode().set(ExtendedHeaders.ON_DEMAND.name()))
                 .setRestartAllServices()
                 .build();

   static final SimpleAttributeDefinition MAX_CONTENT_LENGTH =
         new SimpleAttributeDefinitionBuilder(ModelKeys.MAX_CONTENT_LENGTH, ModelType.INT, true)
               .setAllowExpression(true)
               .setXmlName(ModelKeys.MAX_CONTENT_LENGTH)
               .setRestartAllServices()
               .setDefaultValue(new ModelNode().set(RestServerConfigurationBuilder.DEFAULT_MAX_CONTENT_LENGTH))
               .build();

   static final SimpleAttributeDefinition[] REST_ATTRIBUTES = { SOCKET_BINDING, CONTEXT_PATH, EXTENDED_HEADERS, MAX_CONTENT_LENGTH };

   public RestConnectorResource(boolean isRuntimeRegistration) {
      super(REST_CONNECTOR_PATH, EndpointExtension.getResourceDescriptionResolver(ModelKeys.REST_CONNECTOR), RestSubsystemAdd.INSTANCE, RestSubsystemRemove.INSTANCE, isRuntimeRegistration);
   }

   @Override
   public void registerChildren(ManagementResourceRegistration resourceRegistration) {
      resourceRegistration.registerSubModel(new RestAuthenticationResource());
      resourceRegistration.registerSubModel(new EncryptionResource());
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
