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

import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.rest.RestServer;
import org.jboss.as.controller.AbstractRuntimeOnlyHandler;
import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.OperationFailedException;
import org.jboss.as.controller.descriptions.ModelDescriptionConstants;
import org.jboss.as.controller.registry.ManagementResourceRegistration;
import org.jboss.dmr.ModelNode;
import org.jboss.msc.service.ServiceController;

public class RestMetricsHandler extends AbstractRuntimeOnlyHandler {
   final String name;

   protected RestMetricsHandler(final String name) {
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
         RestService service = (RestService) controller.getService();
         RestServer server = service.getValue();
         ModelNode result = new ModelNode();
         switch (metric) {
            case BYTES_READ:
               result.set(server.getTransport().getTotalBytesRead());
               break;
            case BYTES_WRITTEN:
               result.set(server.getTransport().getTotalBytesWritten());
               break;
            case TRANSPORT_RUNNING:
               result.set(server.getTransportStatus() == ComponentStatus.RUNNING);
               break;
         }
         context.getResult().set(result);
      }
      context.stepCompleted();
   }

   protected static void registerMetrics(final ManagementResourceRegistration resourceRegistration, final String name) {
      RestMetricsHandler handler = new RestMetricsHandler(name);
      for (ProtocolServerMetrics metric : ProtocolServerMetrics.values()) {
         resourceRegistration.registerMetric(metric.definition, handler);
      }
   }

}
