/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.rhq;

import org.infinispan.rhq.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.mc4j.ems.connection.EmsConnection;
import org.mc4j.ems.connection.bean.EmsBean;
import org.rhq.core.domain.measurement.AvailabilityType;
import org.rhq.core.domain.measurement.DataType;
import org.rhq.core.domain.measurement.MeasurementDataNumeric;
import org.rhq.core.domain.measurement.MeasurementDataTrait;
import org.rhq.core.domain.measurement.MeasurementReport;
import org.rhq.core.domain.measurement.MeasurementScheduleRequest;
import org.rhq.core.pluginapi.inventory.ResourceContext;
import org.rhq.plugins.jmx.JMXComponent;
import org.rhq.plugins.jmx.MBeanResourceComponent;
import org.rhq.plugins.jmx.ObjectNameQueryUtility;

import javax.management.ObjectName;
import java.util.List;
import java.util.Set;

import static org.infinispan.jmx.CacheManagerJmxRegistration.CACHE_MANAGER_JMX_GROUP;

/**
 * The component class for the Infinispan manager
 *
 * @author Heiko W. Rupp
 * @author Galder Zamarreño
 */
public class CacheManagerComponent extends MBeanResourceComponent<MBeanResourceComponent> {
   private static final Log log = LogFactory.getLog(CacheManagerComponent.class, Log.class);
   protected ResourceContext<JMXComponent> context;
   private String cacheManagerPattern;

   /**
    * Return availability of this resource. We do this by checking the connection to it. If the Manager would expose
    * some "run state" we could check for that too.
    *
    * @see org.rhq.core.pluginapi.inventory.ResourceComponent#getAvailability()
    */
   public AvailabilityType getAvailability() {
      boolean trace = log.isTraceEnabled();
      EmsConnection conn = getEmsConnection();
      try {
         conn.refresh();
         EmsBean bean = queryCacheManagerBean(conn);
         if (bean != null) {
            bean.refreshAttributes();
            if (trace) log.tracef("Cache manager %s could be found and attributes where refreshed, so it's up.", bean);
            return AvailabilityType.UP;
         }
         if (trace) log.trace("Cache manager could not be found, so cache manager is down");
         return AvailabilityType.DOWN;
      } catch (Exception e) {
         if (trace) log.trace("There was an exception checking availability, so cache manager is down");
         return AvailabilityType.DOWN;
      }
   }

   /**
    * Start the resource connection
    */
   @SuppressWarnings("unchecked")
   public void start(ResourceContext context) {
      // TODO: Call super.start() ?
      this.context = context;
      this.cacheManagerPattern = "*:" + CACHE_MANAGER_JMX_GROUP + ",name=" + ObjectName.quote(context.getResourceKey()) + ",*";
   }

   @Override
   public EmsConnection getEmsConnection() {
      return context.getParentResourceComponent().getEmsConnection();
   }

   /**
    * Gather measurement data
    *
    * @see org.rhq.core.pluginapi.measurement.MeasurementFacet#getValues(org.rhq.core.domain.measurement.MeasurementReport,
    *      java.util.Set)
    */
   public void getValues(MeasurementReport report, Set<MeasurementScheduleRequest> metrics) {
      boolean trace = log.isTraceEnabled();
      if (trace) log.tracef("Get values for these metrics: %s", metrics);
      EmsConnection conn = getEmsConnection();
      if (trace) log.tracef("Connection to ems server established: %s", conn);
      EmsBean bean = queryCacheManagerBean(conn);
      bean.refreshAttributes();
      if (trace) log.tracef("Querying returned bean: %s", bean);
      for (MeasurementScheduleRequest req : metrics) {
         DataType type = req.getDataType();
         if (type == DataType.MEASUREMENT) {
            String tmp = (String) bean.getAttribute(req.getName()).getValue();
            Double val = Double.valueOf(tmp);
            if (trace) log.tracef("Metric (%s) is measurement with value %s", req.getName(), val);
            MeasurementDataNumeric res = new MeasurementDataNumeric(req, val);
            report.addData(res);
         } else if (type == DataType.TRAIT) {
            String value = (String) bean.getAttribute(req.getName()).getValue();
            if (trace) log.tracef("Metric (%s) is trait with value %s", req.getName(), value);
            MeasurementDataTrait res = new MeasurementDataTrait(req, value);
            report.addData(res);
         }
      }
   }

   private EmsBean queryCacheManagerBean(EmsConnection conn) {
      String pattern = cacheManagerPattern;
      if (log.isTraceEnabled()) log.tracef("Pattern to query is %s", pattern);
      ObjectNameQueryUtility queryUtility = new ObjectNameQueryUtility(pattern);
      List<EmsBean> beans = conn.queryBeans(queryUtility.getTranslatedQuery());
      if (beans.size() > 1) {
         // If more than one are returned, most likely is due to duplicate domains which is not the general case
         log.moreThanOneBeanReturned(pattern, beans);
      }
      return beans.get(0);
   }
}
