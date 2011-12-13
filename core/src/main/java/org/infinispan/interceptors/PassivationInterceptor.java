/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
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
package org.infinispan.interceptors;

import org.infinispan.commands.write.EvictCommand;
import org.infinispan.container.DataContainer;
import org.infinispan.context.InvocationContext;
import org.infinispan.eviction.PassivationManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.base.JmxStatsCommandInterceptor;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.rhq.helpers.pluginAnnotations.agent.MeasurementType;
import org.rhq.helpers.pluginAnnotations.agent.Metric;
import org.rhq.helpers.pluginAnnotations.agent.Operation;

/**
 * Writes evicted entries back to the store on the way in through the CacheStore
 *
 * @since 4.0
 */
@MBean(objectName = "Passivation", description = "Component that handles passivating entries to a CacheStore on eviction.")
public class PassivationInterceptor extends JmxStatsCommandInterceptor {
   

   PassivationManager passivator;
   DataContainer dataContainer;

   private static final Log log = LogFactory.getLog(PassivationInterceptor.class);

   @Override
   protected Log getLog() {
      return log;
   }

   @Inject
   public void setDependencies(PassivationManager passivator, DataContainer dataContainer) {
      this.passivator = passivator;
      this.dataContainer = dataContainer;
   }

   @Override
   public Object visitEvictCommand(InvocationContext ctx, EvictCommand command) throws Throwable {
      Object key = command.getKey();
      passivator.passivate(dataContainer.get(key));
      return invokeNextInterceptor(ctx, command);
   }

   @ManagedOperation(description = "Resets statistics gathered by this component")
   @Operation(displayName = "Reset statistics")
   public void resetStatistics() {
      passivator.resetPassivationCount();
   }

   @ManagedAttribute(description = "Number of passivation events")
   @Metric(displayName = "Number of cache passivations", measurementType = MeasurementType.TRENDSUP)   
   public String getPassivations() {
      if (!getStatisticsEnabled()) return "N/A";
      return String.valueOf(passivator.getPassivationCount());
   }
}
