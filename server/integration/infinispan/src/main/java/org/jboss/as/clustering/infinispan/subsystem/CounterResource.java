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

import static org.jboss.as.controller.PathAddress.pathAddress;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.OP_ADDR;

import java.util.Optional;

import org.infinispan.counter.api.CounterManager;
import org.infinispan.counter.api.Storage;
import org.infinispan.counter.api.StrongCounter;
import org.infinispan.counter.api.WeakCounter;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.infinispan.SecurityActions;
import org.jboss.as.controller.AbstractAddStepHandler;
import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.OperationDefinition;
import org.jboss.as.controller.OperationFailedException;
import org.jboss.as.controller.OperationStepHandler;
import org.jboss.as.controller.PathAddress;
import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.ReloadRequiredWriteAttributeHandler;
import org.jboss.as.controller.SimpleAttributeDefinitionBuilder;
import org.jboss.as.controller.SimpleOperationDefinitionBuilder;
import org.jboss.as.controller.SimpleResourceDefinition;
import org.jboss.as.controller.descriptions.ResourceDescriptionResolver;
import org.jboss.as.controller.registry.AttributeAccess;
import org.jboss.as.controller.registry.ManagementResourceRegistration;
import org.jboss.as.controller.services.path.ResolvePathHandler;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;

/**
 * Resource description for the addressable resource
 * /subsystem=infinispan/cache-container=X/counter=*
 *
 * @author Pedro Ruivo
 * @author Vladimir Blagojevic
 * @since 9.2
 */
public class CounterResource extends SimpleResourceDefinition {

   //atributes
   static final AttributeDefinition COUNTER_NAME = new SimpleAttributeDefinitionBuilder(ModelKeys.NAME,
         ModelType.STRING, false)
         .setXmlName(Attribute.NAME.getLocalName())
         .setAllowExpression(false)
         .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
         .build();

   static final AttributeDefinition STORAGE = new SimpleAttributeDefinitionBuilder(ModelKeys.STORAGE, 
         ModelType.STRING, false)
         .setXmlName(Attribute.STORAGE.getLocalName())
         .setAllowExpression(false)
         .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
         .setAllowedValues(Storage.VOLATILE.toString(), Storage.PERSISTENT.toString())
         .build();

   //define but don't register
   static final AttributeDefinition TYPE = new SimpleAttributeDefinitionBuilder(ModelKeys.TYPE, ModelType.STRING, false)
         .setXmlName(Attribute.TYPE.getLocalName())
         .setAllowExpression(false)
         .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
         .build();

   static final AttributeDefinition INITIAL_VALUE = new SimpleAttributeDefinitionBuilder(ModelKeys.INITIAL_VALUE,
         ModelType.LONG, true)
         .setXmlName(Attribute.INITIAL_VALUE.getLocalName())
         .setAllowExpression(false)
         .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
         .build();

   static final AttributeDefinition[] ATTRIBUTES = { COUNTER_NAME, STORAGE, INITIAL_VALUE };

   // operations

   private static final OperationDefinition COUNTER_RESET = buildOperation("counter-reset").build();

   private final boolean runtimeRegistration;
   private final ResolvePathHandler resolvePathHandler;

   public CounterResource(PathElement pathElement, ResourceDescriptionResolver descriptionResolver,
         ResolvePathHandler resolvePathHandler, AbstractAddStepHandler addHandler, OperationStepHandler removeHandler,
         boolean runtimeRegistration) {
      super(pathElement, descriptionResolver, addHandler, removeHandler);
      this.resolvePathHandler = resolvePathHandler;
      this.runtimeRegistration = runtimeRegistration;
   }

   @Override
   public void registerAttributes(ManagementResourceRegistration resourceRegistration) {
      super.registerAttributes(resourceRegistration);
      final OperationStepHandler writeHandler = new ReloadRequiredWriteAttributeHandler(ATTRIBUTES);
      for (AttributeDefinition attr : ATTRIBUTES) {
         resourceRegistration.registerReadWriteAttribute(attr, null, writeHandler);
      }

      if (runtimeRegistration) {
         CounterMetricsHandler.INSTANCE.registerMetrics(resourceRegistration);
      }
   }

   @Override
   public void registerOperations(ManagementResourceRegistration resourceRegistration) {
      //TODO register reset op here
      super.registerOperations(resourceRegistration);
      resourceRegistration.registerOperationHandler(CounterResource.COUNTER_RESET, CounterResetCommand.INSTANCE);
   }

   private static SimpleOperationDefinitionBuilder buildOperation(String name) {
      return new SimpleOperationDefinitionBuilder(name, new InfinispanResourceDescriptionResolver(ModelKeys.COUNTERS))
            .setRuntimeOnly();
   }

   private static PathElement counterElement(OperationContext context, ModelNode operation)
         throws OperationFailedException {
      final PathAddress address = pathAddress(operation.require(OP_ADDR));
      final PathElement counterElement = address.getElement(address.size() - 1);
      return counterElement;
   }

   private static String counterName(OperationContext context, ModelNode operation) throws OperationFailedException {
      PathElement counterElement = counterElement(context, operation);
      return counterElement.getValue();
   }

   private static String counterType(OperationContext context, ModelNode operation) throws OperationFailedException {
      PathElement counterElement = counterElement(context, operation);
      return counterElement.getKey();
   }

   private static Optional<Long> findLong(AttributeDefinition definition, OperationContext context, ModelNode operation)
         throws OperationFailedException {
      ModelNode aLong = definition.resolveModelAttribute(context, operation);
      return aLong.isDefined() ? Optional.of(aLong.asLong()) : Optional.empty();
   }

   private static OperationFailedException counterManagerNotFound() {
      return new OperationFailedException("CounterManager not found in server.");
   }

   private static OperationFailedException counterNotFound(String name) {
      return new OperationFailedException("Counter '" + name + "' not defined.");
   }

   public static class CounterRemoveCommand extends BaseCounterManagerCommand {
      public static final CounterRemoveCommand INSTANCE = new CounterRemoveCommand();

      private CounterRemoveCommand() {
         super();
      }

      @Override
      protected ModelNode invoke(CounterManager counterManager, OperationContext context, ModelNode operation)
            throws Exception {
         final String counterName = counterName(context, operation);
         counterManager.remove(counterName);
         return new ModelNode();
      }
   }

   private static class CounterResetCommand extends BaseCounterManagerCommand {
      private static final CounterResetCommand INSTANCE = new CounterResetCommand();

      private CounterResetCommand() {
         super();
      }

      @Override
      protected ModelNode invoke(CounterManager counterManager, OperationContext context, ModelNode operation)
            throws Exception {
         final String counterName = counterName(context, operation);
         final String counterType = counterType(context, operation);
         if (counterManager.isDefined(counterName)) {
            boolean isStrongCounter = ModelKeys.STRONG_COUNTER.equals(counterType);
            if (isStrongCounter) {
               StrongCounter strongCounter = counterManager.getStrongCounter(counterName);
               strongCounter.reset();
            } else {
               WeakCounter weakCounter = counterManager.getWeakCounter(counterName);
               weakCounter.reset();
            }
         }
         return new ModelNode();
      }
   }

   private static abstract class BaseCounterManagerCommand extends CacheContainerCommands {

      BaseCounterManagerCommand() {
         //path to container from counter address has two elements
         super(2);
      }

      abstract ModelNode invoke(CounterManager counterManager, OperationContext context, ModelNode operation)
            throws Exception;

      @Override
      protected final ModelNode invokeCommand(EmbeddedCacheManager cacheManager, OperationContext context,
            ModelNode operation) throws Exception {
         Optional<CounterManager> optCounterManager = SecurityActions.findCounterManager(cacheManager);
         CounterManager counterManager = optCounterManager.orElseThrow(CounterResource::counterManagerNotFound);
         return invoke(counterManager, context, operation);
      }
   }
}
