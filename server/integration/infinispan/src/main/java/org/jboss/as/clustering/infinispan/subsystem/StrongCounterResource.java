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

import org.infinispan.counter.api.CounterType;
import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.OperationStepHandler;
import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.ReloadRequiredWriteAttributeHandler;
import org.jboss.as.controller.SimpleAttributeDefinitionBuilder;
import org.jboss.as.controller.registry.AttributeAccess;
import org.jboss.as.controller.registry.ManagementResourceRegistration;
import org.jboss.as.controller.services.path.ResolvePathHandler;
import org.jboss.dmr.ModelType;

/**
 * Resource description for the addressable resource
 * /subsystem=infinispan/cache-container=X/counter=*
 *
 * @author Pedro Ruivo
 * @author Vladimir Blagojevic
 * @since 9.2
 */
public class StrongCounterResource extends CounterResource {
   
   public static final PathElement PATH = PathElement.pathElement(ModelKeys.STRONG_COUNTER);

   static final AttributeDefinition LOWER_BOUND = 
         new SimpleAttributeDefinitionBuilder(ModelKeys.LOWER_BOUND, ModelType.LONG, true)
         .setXmlName(Attribute.LOWER_BOUND.getLocalName())
         .setAllowExpression(false)
         .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
         .build();         
   
   static final AttributeDefinition UPPER_BOUND = 
         new SimpleAttributeDefinitionBuilder(ModelKeys.UPPER_BOUND, ModelType.LONG, true)
         .setXmlName(Attribute.UPPER_BOUND.getLocalName())
         .setAllowExpression(false)
         .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
         .build();     
   
   static final AttributeDefinition TYPE = 
         new SimpleAttributeDefinitionBuilder(ModelKeys.TYPE, ModelType.STRING, false)
            .setXmlName(Attribute.TYPE.getLocalName())
            .setAllowExpression(false)
            .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
            .setAllowedValues(CounterType.BOUNDED_STRONG.toString(), CounterType.UNBOUNDED_STRONG.toString())
            .build();            
   
   static final AttributeDefinition[] STRONG_ATTRIBUTES = { LOWER_BOUND, TYPE, UPPER_BOUND };
     
   public StrongCounterResource(ResolvePathHandler resolvePathHandler, boolean runtimeRegistration) {
      super(StrongCounterResource.PATH, 
            new InfinispanResourceDescriptionResolver(ModelKeys.COUNTERS), 
            resolvePathHandler, new StrongCounterAddHandler(), new CounterRemoveHandler(), runtimeRegistration);
   }

   @Override
   public void registerAttributes(ManagementResourceRegistration resourceRegistration) {
      super.registerAttributes(resourceRegistration);
      
      final OperationStepHandler writeHandler = new ReloadRequiredWriteAttributeHandler(STRONG_ATTRIBUTES);
      for (AttributeDefinition attr : STRONG_ATTRIBUTES) {
          resourceRegistration.registerReadWriteAttribute(attr, null, writeHandler);
      }
   }

   @Override
   public void registerOperations(ManagementResourceRegistration resourceRegistration) {
      super.registerOperations(resourceRegistration);      
   }
}
