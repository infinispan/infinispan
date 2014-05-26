/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012-2013 Red Hat Inc. and/or its affiliates and other contributors
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

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

import org.jboss.as.controller.AbstractRuntimeOnlyHandler;
import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.OperationFailedException;
import org.jboss.as.controller.SimpleAttributeDefinitionBuilder;
import org.jboss.as.controller.descriptions.ModelDescriptionConstants;
import org.jboss.as.controller.registry.ManagementResourceRegistration;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;
import org.jboss.msc.service.ServiceController;

public class ProtocolServerMetricsHandler extends AbstractRuntimeOnlyHandler {
   final String name;

   public enum ProtocolServerMetrics {
      BYTES_READ(new SimpleAttributeDefinitionBuilder(MetricKeys.BYTES_READ, ModelType.LONG, true).setStorageRuntime().build()),
      BYTES_WRITTEN(new SimpleAttributeDefinitionBuilder(MetricKeys.BYTES_WRITTEN, ModelType.LONG, true).setStorageRuntime().build());

      private static final Map<String, ProtocolServerMetrics> MAP = new HashMap<String, ProtocolServerMetrics>();
      static {
         for (ProtocolServerMetrics stat : EnumSet.allOf(ProtocolServerMetrics.class)) {
            MAP.put(stat.toString(), stat);
         }
      }
      final AttributeDefinition definition;

      private ProtocolServerMetrics(final AttributeDefinition definition) {
         this.definition = definition;
      }

      @Override
      public final String toString() {
         return definition.getName();
      }

      public static ProtocolServerMetrics getStat(final String stringForm) {
         return MAP.get(stringForm);
      }
   }

   protected ProtocolServerMetricsHandler(final String name) {
      this.name = name;
   }

   @Override
   protected void executeRuntimeStep(OperationContext context, ModelNode operation) throws OperationFailedException {
      final String attrName = operation.require(ModelDescriptionConstants.NAME).asString();
      ProtocolServerMetrics metric = ProtocolServerMetrics.getStat(attrName);
      if (metric == null) {
         context.getFailureDescription().set(String.format("Unknown metric %s", attrName));
      } else {
         final ServiceController<?> controller = context.getServiceRegistry(false)
               .getService(EndpointUtils.getServiceName(operation, name));
         ProtocolServerService service = (ProtocolServerService) controller.getService();
         ModelNode result = new ModelNode();
         switch (metric) {
         case BYTES_READ:
            result.set(service.getTransport().getTotalBytesRead());
            break;
         case BYTES_WRITTEN:
            result.set(service.getTransport().getTotalBytesWritten());
            break;
         }
         context.getResult().set(result);
      }
      context.stepCompleted();
   }

   protected static void registerMetrics(final ManagementResourceRegistration resourceRegistration, final String name) {
      ProtocolServerMetricsHandler handler = new ProtocolServerMetricsHandler(name);
      for (ProtocolServerMetrics metric : ProtocolServerMetrics.values()) {
         resourceRegistration.registerMetric(metric.definition, handler);
      }
   }

}
