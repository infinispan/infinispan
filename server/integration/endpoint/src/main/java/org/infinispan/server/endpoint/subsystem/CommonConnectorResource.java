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

import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.OperationStepHandler;
import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.ReloadRequiredWriteAttributeHandler;
import org.jboss.as.controller.SimpleAttributeDefinition;
import org.jboss.as.controller.SimpleAttributeDefinitionBuilder;
import org.jboss.as.controller.SimpleListAttributeDefinition;
import org.jboss.as.controller.SimpleResourceDefinition;
import org.jboss.as.controller.descriptions.ResourceDescriptionResolver;
import org.jboss.as.controller.registry.AttributeAccess;
import org.jboss.as.controller.registry.ManagementResourceRegistration;
import org.jboss.dmr.ModelType;

/**
 * CommonConnectorResource.
 *
 * @author Tristan Tarrant
 * @since 5.3
 */
public abstract class CommonConnectorResource extends SimpleResourceDefinition {
   static final SimpleAttributeDefinition NAME =
         new SimpleAttributeDefinitionBuilder(ModelKeys.NAME, ModelType.STRING, true)
                 .setAllowExpression(true)
                 .setXmlName(ModelKeys.NAME)
                 .setRestartAllServices()
                 .build();

   static final SimpleAttributeDefinition CACHE_CONTAINER =
         new SimpleAttributeDefinitionBuilder(ModelKeys.CACHE_CONTAINER, ModelType.STRING, false)
                 .setAllowExpression(true)
                 .setXmlName(ModelKeys.CACHE_CONTAINER)
                 .setRestartAllServices()
                 .build();

   static final AttributeDefinition IGNORED_CACHE =
          new SimpleAttributeDefinitionBuilder(ModelKeys.IGNORED_CACHE, ModelType.STRING, true)
                  .setXmlName(ModelKeys.IGNORED_CACHE)
                  .setAllowExpression(false)
                  .setFlags(AttributeAccess.Flag.RESTART_NONE)
                  .build();

   static final SimpleListAttributeDefinition IGNORED_CACHES = SimpleListAttributeDefinition.Builder.of(ModelKeys.IGNORED_CACHES, IGNORED_CACHE)
           .setFlags(AttributeAccess.Flag.RESTART_NONE)
           .setAllowNull(true)
           .build();

   static final SimpleAttributeDefinition[] COMMON_CONNECTOR_ATTRIBUTES = {NAME, CACHE_CONTAINER};
   static final SimpleListAttributeDefinition[] COMMON_LIST_CONNECTOR_ATTRIBUTES = {IGNORED_CACHES};

   private final boolean runtimeRegistration;

   public CommonConnectorResource(PathElement pathElement, ResourceDescriptionResolver descriptionResolver, OperationStepHandler addHandler, OperationStepHandler removeHandler, boolean runtimeRegistration) {
      super(pathElement, descriptionResolver, addHandler, removeHandler);
      this.runtimeRegistration = runtimeRegistration;
   }

   @Override
   public void registerAttributes(ManagementResourceRegistration resourceRegistration) {
      super.registerAttributes(resourceRegistration);

      final OperationStepHandler writeHandler = new ReloadRequiredWriteAttributeHandler(COMMON_CONNECTOR_ATTRIBUTES);
      for (AttributeDefinition attr : COMMON_CONNECTOR_ATTRIBUTES) {
         resourceRegistration.registerReadWriteAttribute(attr, null, writeHandler);
      }
      resourceRegistration.registerReadWriteAttribute(IGNORED_CACHES, null, new CacheIgnoreReadWriteHandler(IGNORED_CACHES));
   }

   protected boolean isRuntimeRegistration() {
      return runtimeRegistration;
   }
}
