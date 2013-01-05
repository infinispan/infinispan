/*
 * Copyright 2012 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */

package org.infinispan.server.core.transport;

import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.rhq.helpers.pluginAnnotations.agent.DataType;
import org.rhq.helpers.pluginAnnotations.agent.DisplayType;
import org.rhq.helpers.pluginAnnotations.agent.MeasurementType;
import org.rhq.helpers.pluginAnnotations.agent.Metric;

/**
 * Server transport abstraction
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
@Scope(Scopes.GLOBAL) // So that RHQ plugin generation detects this as a cache manager level component
@MBean(objectName = "Transport",
       description = "Transport component manages read and write operations to/from server.")
public interface Transport {
   
   void start();
   
   void stop();

   @ManagedAttribute(description = "Returns the total number of bytes written " +
         "by the server back to clients which includes both protocol and user information.")
   @Metric(displayName = "Number of total number of bytes written",
           measurementType = MeasurementType.TRENDSUP,
           displayType = DisplayType.SUMMARY)
   String getTotalBytesWritten();

   @ManagedAttribute(description = "Returns the total number of bytes read " +
         "by the server from clients which includes both protocol and user information.")
   @Metric(displayName = "Number of total number of bytes read",
           measurementType = MeasurementType.TRENDSUP,
           displayType = DisplayType.SUMMARY)
   String getTotalBytesRead();

   @ManagedAttribute(description = "Returns the host to which the transport binds.")
   @Metric(displayName = "Host name", dataType = DataType.TRAIT,
           displayType = DisplayType.SUMMARY)
   @SuppressWarnings("unused")
   String getHostName();

   @ManagedAttribute(description = "Returns the port to which the transport binds.")
   @Metric(displayName = "Port", dataType = DataType.TRAIT,
           displayType = DisplayType.SUMMARY)
   String getPort();

   @ManagedAttribute(description = "Returns the number of worker threads.")
   @Metric(displayName = "Number of worker threads", dataType = DataType.TRAIT,
           displayType = DisplayType.SUMMARY)
   @SuppressWarnings("unused")
   String getNumberWorkerThreads();

   @ManagedAttribute(description = "Returns the idle timeout.")
   @Metric(displayName = "Idle timeout", dataType = DataType.TRAIT,
           displayType = DisplayType.SUMMARY)
   @SuppressWarnings("unused")
   String getIdleTimeout();

   @ManagedAttribute(description = "Returns whether TCP no delay was configured or not.")
   @Metric(displayName = "TCP no delay", dataType = DataType.TRAIT,
           displayType = DisplayType.SUMMARY)
   @SuppressWarnings("unused")
   String getTcpNoDelay();

   @ManagedAttribute(description = "Returns the send buffer size.")
   @Metric(displayName = "Send buffer size", dataType = DataType.TRAIT,
           displayType = DisplayType.SUMMARY)
   @SuppressWarnings("unused")
   String getSendBufferSize();

   @ManagedAttribute(description = "Returns the receive buffer size.")
   @Metric(displayName = "Receive buffer size", dataType = DataType.TRAIT,
           displayType = DisplayType.SUMMARY)
   @SuppressWarnings("unused")
   String getReceiveBufferSize();

   @ManagedAttribute(description = "Returns a count of active connections this server.")
   @Metric(displayName = "Local active connections",
         dataType = DataType.MEASUREMENT, displayType = DisplayType.SUMMARY)
   @SuppressWarnings("unused")
   Integer getNumberOfLocalConnections();

   @ManagedAttribute(description = "Returns a count of active connections in the cluster. " +
         "This operation will make remote calls to aggregate results, " +
         "so latency might have an impact on the speed of calculation of this attribute.")
   @Metric(displayName = "Cluster-wide number of active connections",
         dataType = DataType.MEASUREMENT, displayType = DisplayType.SUMMARY)
   @SuppressWarnings("unused")
   Integer getNumberOfGlobalConnections();

}
