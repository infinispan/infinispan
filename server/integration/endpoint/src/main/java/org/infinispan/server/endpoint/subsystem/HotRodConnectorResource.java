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

import org.infinispan.server.hotrod.configuration.HotRodServerConfiguration;
import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.OperationStepHandler;
import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.ReloadRequiredWriteAttributeHandler;
import org.jboss.as.controller.ResourceDefinition;
import org.jboss.as.controller.SimpleAttributeDefinition;
import org.jboss.as.controller.SimpleAttributeDefinitionBuilder;
import org.jboss.as.controller.registry.ManagementResourceRegistration;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;

/**
 * HotRodConnectorResource.
 *
 * @author Tristan Tarrant
 * @since 5.3
 */
public class HotRodConnectorResource extends ProtocolServerConnectorResource implements ResourceDefinition {

   public static final PathElement HOTROD_CONNECTOR_PATH = PathElement.pathElement(ModelKeys.HOTROD_CONNECTOR);

   static final SimpleAttributeDefinition WORKER_THREADS =
         new SimpleAttributeDefinitionBuilder(ModelKeys.WORKER_THREADS, ModelType.INT, true)
               .setAllowExpression(true)
               .setXmlName(ModelKeys.WORKER_THREADS)
               .setDefaultValue(new ModelNode().set(HotRodServerConfiguration.WORKER_THREADS.getDefaultValue()))
               .setRestartAllServices()
               .build();

   static final SimpleAttributeDefinition[] HOTROD_ATTRIBUTES = {WORKER_THREADS};

   public HotRodConnectorResource(boolean isRuntimeRegistration) {
      super(HOTROD_CONNECTOR_PATH, EndpointExtension.getResourceDescriptionResolver(ModelKeys.HOTROD_CONNECTOR), HotRodSubsystemAdd.INSTANCE, HotRodSubsystemRemove.INSTANCE, isRuntimeRegistration);
   }

   @Override
   public void registerChildren(ManagementResourceRegistration resourceRegistration) {
      resourceRegistration.registerSubModel(new TopologyStateTransferResource());
      resourceRegistration.registerSubModel(new AuthenticationResource());
      resourceRegistration.registerSubModel(new EncryptionResource());
   }

   @Override
   public void registerAttributes(ManagementResourceRegistration resourceRegistration) {
      super.registerAttributes(resourceRegistration);

      final OperationStepHandler writeHandler = new ReloadRequiredWriteAttributeHandler(HOTROD_ATTRIBUTES);
      for (AttributeDefinition attr : HOTROD_ATTRIBUTES) {
         resourceRegistration.registerReadWriteAttribute(attr, null, writeHandler);
      }

      if (isRuntimeRegistration()) {
         ProtocolServerMetricsHandler.registerMetrics(resourceRegistration, "hotrod");
      }
   }



}
