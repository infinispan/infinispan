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

import static org.infinispan.rhq.RhqUtil.constructNumericMeasure;

import java.util.List;
import java.util.Set;

import javax.management.ObjectName;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.mc4j.ems.connection.EmsConnection;
import org.mc4j.ems.connection.bean.EmsBean;
import org.mc4j.ems.connection.bean.attribute.EmsAttribute;
import org.rhq.core.domain.measurement.AvailabilityType;
import org.rhq.core.domain.measurement.DataType;
import org.rhq.core.domain.measurement.MeasurementDataNumeric;
import org.rhq.core.domain.measurement.MeasurementDataTrait;
import org.rhq.core.domain.measurement.MeasurementReport;
import org.rhq.core.domain.measurement.MeasurementScheduleRequest;
import org.rhq.core.pluginapi.inventory.ResourceContext;
import org.rhq.plugins.jmx.JMXServerComponent;
import org.rhq.plugins.jmx.MBeanResourceComponent;
import org.rhq.plugins.jmx.ObjectNameQueryUtility;

/**
 * The component class for the Infinispan manager
 *
 * @author Heiko W. Rupp
 * @author Galder Zamarreño
 */
public class CacheManagerComponent extends MBeanResourceComponent<JMXServerComponent<?>> {
   private static final Log log = LogFactory.getLog(CacheManagerComponent.class);
   protected ResourceContext<JMXServerComponent<?>> context;
   private String cacheManagerPattern;

   /**
    * Return availability of this resource. We do this by checking the connection to it. If the Manager would expose
    * some "run state" we could check for that too.
    *
    * @see org.rhq.core.pluginapi.inventory.ResourceComponent#getAvailability()
    */
   @Override
   public AvailabilityType getAvailability() {
      boolean trace = log.isTraceEnabled();
      EmsConnection conn = getEmsConnection();
      try {
         conn.refresh();
         EmsBean bean = queryCacheManagerBean(conn);
         if (bean != null) {
            bean.refreshAttributes();
            if (trace) log.trace("Cache manager "+bean+" could be found and attributes where refreshed, so it's up.");
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
   @Override
   public void start(ResourceContext<JMXServerComponent<?>> context) {
      // TODO: Call super.start() ?
      this.context = context;
      this.cacheManagerPattern = "*:" + CacheManagerDiscovery.CACHE_MANAGER_JMX_GROUP + ",name=" + ObjectName.quote(context.getResourceKey()) + ",*";
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
   @Override
   public void getValues(MeasurementReport report, Set<MeasurementScheduleRequest> metrics) {
      boolean trace = log.isTraceEnabled();
      if (trace) log.trace("Get values for these metrics: " + metrics);
      EmsConnection conn = getEmsConnection();
      if (trace) log.trace("Connection to ems server established: " + conn);
      EmsBean bean = queryCacheManagerBean(conn);
      bean.refreshAttributes();
      if (trace) log.trace("Querying returned bean: " + bean);
      for (MeasurementScheduleRequest req : metrics) {
         DataType type = req.getDataType();
         if (type == DataType.MEASUREMENT) {
            EmsAttribute att = bean.getAttribute(req.getName());
            if (att != null) {
               MeasurementDataNumeric res = constructNumericMeasure(att.getTypeClass(), att.getValue(), req);
               report.addData(res);
            }
         } else if (type == DataType.TRAIT) {
            String value = (String) bean.getAttribute(req.getName()).getValue();
            if (trace) log.trace("Metric ("+req.getName()+") is trait with value "+ value);
            MeasurementDataTrait res = new MeasurementDataTrait(req, value);
            report.addData(res);
         }
      }
   }

   private EmsBean queryCacheManagerBean(EmsConnection conn) {
      String pattern = cacheManagerPattern;
      if (log.isTraceEnabled()) log.trace("Pattern to query is " + pattern);
      ObjectNameQueryUtility queryUtility = new ObjectNameQueryUtility(pattern);
      List<EmsBean> beans = conn.queryBeans(queryUtility.getTranslatedQuery());
      if (beans.size() > 1) {
         // If more than one are returned, most likely is due to duplicate domains which is not the general case
         if(log.isWarnEnabled()) {
            log.warn(String.format("More than one bean returned from applying %s pattern: %s", pattern, beans));
         }
      }
      return beans.get(0);
   }
}
