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

import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.OP_ADDR;

import java.util.Collection;
import java.util.List;

import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.counter.EmbeddedCounterManagerFactory;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterConfiguration.Builder;
import org.infinispan.counter.api.CounterManager;
import org.infinispan.counter.api.CounterType;
import org.infinispan.counter.api.Storage;
import org.infinispan.counter.configuration.AbstractCounterConfiguration;
import org.infinispan.counter.configuration.CounterConfigurationBuilder;
import org.infinispan.counter.configuration.CounterManagerConfigurationBuilder;
import org.infinispan.counter.configuration.StrongCounterConfiguration;
import org.infinispan.counter.configuration.WeakCounterConfiguration;
import org.infinispan.server.infinispan.spi.service.CacheContainerServiceName;
import org.jboss.as.clustering.infinispan.DefaultCacheContainer;
import org.jboss.as.clustering.infinispan.subsystem.CacheConfigurationAdd.Dependency;
import org.jboss.as.controller.AbstractAddStepHandler;
import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.OperationFailedException;
import org.jboss.as.controller.PathAddress;
import org.jboss.as.controller.registry.Resource;
import org.jboss.dmr.ModelNode;
import org.jboss.msc.service.ServiceController;

/**
 * Add operation handler for /subsystem=infinispan/cache-container=clustered/counter=*
 *
 * @author Vladimir Blagojevic
 *
 */
public class CounterAddHandler extends AbstractAddStepHandler {

   CounterAddHandler() {
      super();
   }

   @Override
   protected void performRuntime(OperationContext context, ModelNode operation, ModelNode model)
         throws OperationFailedException {

      ModelNode counterModel = Resource.Tools.readModel(context.readResource(PathAddress.EMPTY_ADDRESS));

      // we need the containerModel
      PathAddress containerAddress = getCacheContainerAddressFromOperation(operation);
      ModelNode containerModel = context.readResourceFromRoot(containerAddress).getModel();

      //install the services from a reusable method
      installRuntimeServices(context, operation, containerModel, counterModel);
   }

   @Override
   protected void populateModel(ModelNode operation, ModelNode model) throws OperationFailedException {
      this.populate(operation, model);
   }

   /**
    * Transfer elements common to both operations and models
    *
    * @param fromModel
    * @param toModel
    */
   void populate(ModelNode fromModel, ModelNode toModel) throws OperationFailedException {
      for (AttributeDefinition attr : CounterResource.ATTRIBUTES) {
         attr.validateAndSet(fromModel, toModel);
      }
   }

   private PathAddress getCacheContainerAddressFromOperation(ModelNode operation) {
      PathAddress counterAddress = getCounterAddressFromOperation(operation);
      return counterAddress.subAddress(0, counterAddress.size() - 2);
   }

   private PathAddress getCounterTypeFromOperation(ModelNode operation) {
      PathAddress counterAddress = getCounterAddressFromOperation(operation);
      int size = counterAddress.size();
      return counterAddress.subAddress(size - 1, size);
   }

   private PathAddress getCounterAddressFromOperation(ModelNode operation) {
      return PathAddress.pathAddress(operation.get(OP_ADDR));
   }

   public Collection<ServiceController<?>> installRuntimeServices(OperationContext context, ModelNode operation,
         ModelNode containerModel, ModelNode counterModel) throws OperationFailedException {
      // get all required addresses, names and service names
      PathAddress counterAddress = getCounterAddressFromOperation(operation);
      PathAddress containerAddress = getCacheContainerAddressFromOperation(operation);
      String counterName = counterAddress.getLastElement().getValue();
      String containerName = containerAddress.getLastElement().getValue();
      String counterType = getCounterTypeFromOperation(operation).getLastElement().getKey();

      GlobalConfigurationBuilder gcBuilder = new GlobalConfigurationBuilder();
      CounterManagerConfigurationBuilder cmcb = new CounterManagerConfigurationBuilder(gcBuilder);

      boolean weakCounter = ModelKeys.WEAK_COUNTER.equals(counterType);
      CounterConfigurationBuilder builder = weakCounter ? cmcb.addWeakCounter() : cmcb.addStrongCounter();
      processModelNode(context, containerName, counterModel, builder, null);
      AbstractCounterConfiguration cc = (AbstractCounterConfiguration) builder.create();

      final ServiceController<?> controller = context.getServiceRegistry(false)
            .getService(CacheContainerServiceName.CACHE_CONTAINER.getServiceName(containerName));
      DefaultCacheContainer cacheManager = (DefaultCacheContainer) controller.getValue();
      CounterManager counterManager = EmbeddedCounterManagerFactory.asCounterManager(cacheManager);
      if (!counterManager.isDefined(counterName)) {
         CounterConfiguration configuration = createCounterConfiguration(cc);
         boolean defineCounter = counterManager.defineCounter(counterName, configuration);
      }
      //TODO complete what else needs to be done here
      return null;
   }

   static void removeRuntimeServices(OperationContext context, ModelNode operation, ModelNode model)
         throws OperationFailedException {
      String name = context.getCurrentAddressValue();
      //TODO
   }

   void processModelNode(OperationContext context, String containerName, ModelNode counter,
         CounterConfigurationBuilder builder, List<Dependency<?>> dependencies) throws OperationFailedException {

      String name = CounterResource.COUNTER_NAME.resolveModelAttribute(context, counter).asString();
      long initialValue = CounterResource.INITIAL_VALUE.resolveModelAttribute(context, counter).asLong();
      String storageType = CounterResource.STORAGE.resolveModelAttribute(context, counter).asString();

      builder.name(name);
      builder.initialValue(initialValue);
      builder.storage(Storage.valueOf(storageType));
   }

   private CounterConfiguration createCounterConfiguration(AbstractCounterConfiguration c) {
      Builder builder = null;
      if (c instanceof StrongCounterConfiguration) {
         StrongCounterConfiguration scc = (StrongCounterConfiguration) c;
         builder = CounterConfiguration
               .builder(scc.isBound() ? CounterType.BOUNDED_STRONG : CounterType.UNBOUNDED_STRONG);
         if (scc.isBound()) {
            builder.lowerBound(scc.lowerBound());
            builder.upperBound(scc.upperBound());
         }
      } else if (c instanceof WeakCounterConfiguration) {
         WeakCounterConfiguration wcc = (WeakCounterConfiguration) c;
         builder = CounterConfiguration.builder(CounterType.WEAK);
         builder.concurrencyLevel(wcc.concurrencyLevel());
      }
      builder.initialValue(c.initialValue());
      builder.storage(c.storage());
      return builder.build();
   }
}
