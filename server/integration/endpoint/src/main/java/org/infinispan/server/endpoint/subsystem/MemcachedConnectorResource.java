/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011-2013 Red Hat Inc. and/or its affiliates and other contributors
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

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OCTET_STREAM_TYPE;

import org.infinispan.server.memcached.configuration.MemcachedServerConfiguration;
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
 * MemcachedConnectorResource.
 *
 * @author Tristan Tarrant
 * @since 5.3
 */
public class MemcachedConnectorResource extends ProtocolServerConnectorResource implements ResourceDefinition {

   public static final PathElement MEMCACHED_CONNECTOR_PATH = PathElement.pathElement(ModelKeys.MEMCACHED_CONNECTOR);

   static final SimpleAttributeDefinition CACHE =
         new SimpleAttributeDefinitionBuilder(ModelKeys.CACHE, ModelType.STRING, true)
                 .setAllowExpression(true)
                 .setXmlName(ModelKeys.CACHE)
                 .setRestartAllServices()
                 .setDefaultValue(new ModelNode().set(MemcachedServerConfiguration.DEFAULT_MEMCACHED_CACHE))
                 .build();

   static final SimpleAttributeDefinition CLIENT_ENCODING =
         new SimpleAttributeDefinitionBuilder(ModelKeys.CLIENT_ENCODING, ModelType.STRING, true)
               .setAllowExpression(true)
               .setXmlName(ModelKeys.CLIENT_ENCODING)
               .setRestartAllServices()
               .setDefaultValue(new ModelNode().set(APPLICATION_OCTET_STREAM_TYPE))
               .build();


   static final SimpleAttributeDefinition[] MEMCACHED_CONNECTOR_ATTRIBUTES = {CACHE, CLIENT_ENCODING};

   public MemcachedConnectorResource(boolean isRuntimeRegistration) {
      super(MEMCACHED_CONNECTOR_PATH, EndpointExtension.getResourceDescriptionResolver(ModelKeys.MEMCACHED_CONNECTOR), MemcachedSubsystemAdd.INSTANCE,
            MemcachedSubsystemRemove.INSTANCE, isRuntimeRegistration);
   }

   @Override
   public void registerAttributes(ManagementResourceRegistration resourceRegistration) {
      super.registerAttributes(resourceRegistration);

      final OperationStepHandler writeHandler = new ReloadRequiredWriteAttributeHandler(MEMCACHED_CONNECTOR_ATTRIBUTES);
      for (AttributeDefinition attr : MEMCACHED_CONNECTOR_ATTRIBUTES) {
         resourceRegistration.registerReadWriteAttribute(attr, null, writeHandler);
      }

      if (isRuntimeRegistration()) {
         ProtocolServerMetricsHandler.registerMetrics(resourceRegistration, "memcached");
      }
   }

}
