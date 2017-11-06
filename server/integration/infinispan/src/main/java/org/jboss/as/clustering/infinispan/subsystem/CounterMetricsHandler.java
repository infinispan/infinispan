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

import org.infinispan.counter.EmbeddedCounterManagerFactory;
import org.infinispan.counter.api.CounterManager;
import org.infinispan.counter.api.StrongCounter;
import org.infinispan.counter.api.WeakCounter;
import org.infinispan.server.infinispan.spi.service.CacheContainerServiceName;
import org.jboss.as.clustering.infinispan.DefaultCacheContainer;
import org.jboss.as.controller.AbstractRuntimeOnlyHandler;
import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.OperationFailedException;
import org.jboss.as.controller.PathAddress;
import org.jboss.as.controller.SimpleAttributeDefinitionBuilder;
import org.jboss.as.controller.registry.ManagementResourceRegistration;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;
import org.jboss.msc.service.ServiceController;

/**
 * Reflects runtime Counter attributes
 *
 * @author Vladimir Blagojevic
 * @since 9.2
 */
public class CounterMetricsHandler extends AbstractRuntimeOnlyHandler {

   public static final CounterMetricsHandler INSTANCE = new CounterMetricsHandler();

   private static final int CACHE_CONTAINER_INDEX = 1;
   private static final int COUNTER_INDEX = 3;

   @Override
   protected void executeRuntimeStep(OperationContext context, ModelNode operation) throws OperationFailedException {

      final ModelNode result = new ModelNode();
      final PathAddress address = PathAddress.pathAddress(operation.require(OP_ADDR));
      final String cacheContainerName = address.getElement(CACHE_CONTAINER_INDEX).getValue();
      final String counterType = address.getElement(COUNTER_INDEX).getKey();
      final String counterName = address.getElement(COUNTER_INDEX).getValue();
      final ServiceController<?> controller = context.getServiceRegistry(false)
            .getService(CacheContainerServiceName.CACHE_CONTAINER.getServiceName(cacheContainerName));

      Long value = null;
      if (controller != null) {
         DefaultCacheContainer cacheManager = (DefaultCacheContainer) controller.getValue();
         CounterManager counterManager = EmbeddedCounterManagerFactory.asCounterManager(cacheManager);
         if (ModelKeys.STRONG_COUNTER.equals(counterType)) {
            StrongCounter sc = counterManager.getStrongCounter(counterName);
            try {
               value = sc.getValue().get();
            } catch (Exception e) {
               throw new OperationFailedException("Can not get value from counter " + sc, e);
            }
         } else if (ModelKeys.WEAK_COUNTER.equals(counterType)) {
            WeakCounter wc = counterManager.getWeakCounter(counterName);
            value = wc.getValue();
         }
         result.set(value);
      }
      context.getResult().set(result);
   }

   public void registerMetrics(ManagementResourceRegistration container) {
      for (CounterMetrics metric : CounterMetrics.values()) {
         container.registerMetric(metric.definition, this);
      }
   }

   public enum CounterMetrics {

      VALUE(MetricKeys.VALUE, ModelType.LONG);

      final AttributeDefinition definition;

      CounterMetrics(String attributeName, ModelType type) {
         this.definition = new SimpleAttributeDefinitionBuilder(attributeName, type)
               .setAllowNull(false)
               .setStorageRuntime().build();
      }

      @Override
      public final String toString() {
         return definition.getName();
      }
   }
}
