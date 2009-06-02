/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
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
package org.infinispan.jopr.infinispan;

import org.mc4j.ems.connection.EmsConnection;
import org.mc4j.ems.connection.bean.EmsBean;
import org.rhq.core.domain.measurement.AvailabilityType;
import org.rhq.core.domain.measurement.MeasurementDataNumeric;
import org.rhq.core.domain.measurement.MeasurementReport;
import org.rhq.core.domain.measurement.MeasurementScheduleRequest;
import org.rhq.core.pluginapi.inventory.InvalidPluginConfigurationException;
import org.rhq.core.pluginapi.inventory.ResourceComponent;
import org.rhq.core.pluginapi.inventory.ResourceContext;
import org.rhq.core.pluginapi.measurement.MeasurementFacet;

import java.util.Set;

/**
 * The component class for the Infinispan manager
 *
 * @author Heiko W. Rupp
 */
public class InfinispanComponent implements ResourceComponent, MeasurementFacet {
   private ResourceContext context;
   private ConnectionHelper helper;


   /**
    * Return availability of this resource. We do this by checking the connection to it. If the Manager would expose
    * some "run state" we could check for that too.
    *
    * @see org.rhq.core.pluginapi.inventory.ResourceComponent#getAvailability()
    */
   public AvailabilityType getAvailability() {
      EmsConnection conn = getConnection();
      try {
         conn.refresh();
         EmsBean bean = conn.getBean(context.getResourceKey());
         if (bean != null)
            bean.refreshAttributes();
         return AvailabilityType.UP;
      } catch (Exception e) {
         return AvailabilityType.DOWN;
      }
   }

   /**
    * Start the resource connection
    *
    * @see org.rhq.core.pluginapi.inventory.ResourceComponent#start(org.rhq.core.pluginapi.inventory.ResourceContext)
    */
   public void start(ResourceContext context) throws InvalidPluginConfigurationException, Exception {

      this.context = context;
      helper = new ConnectionHelper();
      getConnection();
   }

   /**
    * Tear down the rescource connection
    *
    * @see org.rhq.core.pluginapi.inventory.ResourceComponent#stop()
    */
   public void stop() {
      helper.closeConnection();

   }

   /**
    * Gather measurement data
    *
    * @see org.rhq.core.pluginapi.measurement.MeasurementFacet#getValues(org.rhq.core.domain.measurement.MeasurementReport,
    *      java.util.Set)
    */
   public void getValues(MeasurementReport report, Set<MeasurementScheduleRequest> metrics) throws Exception {

      EmsConnection conn = getConnection();
      EmsBean bean = conn.getBean(context.getPluginConfiguration().getSimpleValue("objectName", null));
      bean.refreshAttributes();

      for (MeasurementScheduleRequest req : metrics) {
         // TODO check with Traits in the future - also why are the values Strings?
         String tmp = (String) bean.getAttribute(req.getName()).getValue();
         Double val = Double.valueOf(tmp);
         MeasurementDataNumeric res = new MeasurementDataNumeric(req, val);
         report.addData(res);
      }
   }

   /**
    * Helper to obtain a connection
    *
    * @return EmsConnection object
    */
   protected EmsConnection getConnection() {
      EmsConnection conn = helper.getEmsConnection(context.getPluginConfiguration());
      return conn;
   }

}