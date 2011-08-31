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

import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.rhq.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.mc4j.ems.connection.EmsConnection;
import org.mc4j.ems.connection.bean.EmsBean;
import org.mc4j.ems.connection.bean.attribute.EmsAttribute;
import org.mc4j.ems.connection.bean.operation.EmsOperation;
import org.rhq.core.domain.configuration.Configuration;
import org.rhq.core.domain.configuration.PropertySimple;
import org.rhq.core.domain.measurement.AvailabilityType;
import org.rhq.core.domain.measurement.DataType;
import org.rhq.core.domain.measurement.MeasurementDataNumeric;
import org.rhq.core.domain.measurement.MeasurementDataTrait;
import org.rhq.core.domain.measurement.MeasurementReport;
import org.rhq.core.domain.measurement.MeasurementScheduleRequest;
import org.rhq.core.pluginapi.inventory.ResourceContext;
import org.rhq.core.pluginapi.operation.OperationResult;
import org.rhq.plugins.jmx.MBeanResourceComponent;
import org.rhq.plugins.jmx.ObjectNameQueryUtility;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import static org.infinispan.rhq.RhqUtil.constructNumericMeasure;

/**
 * Component class for Caches within Infinispan
 *
 * @author Heiko W. Rupp
 * @author Galder Zamarreño
 */
public class CacheComponent extends MBeanResourceComponent<CacheManagerComponent> {
   private static final Log log = LogFactory.getLog(CacheComponent.class, Log.class);

   private ResourceContext<CacheManagerComponent> context;
   private String cacheManagerName;
   private String cacheName;

   /**
    * Return availability of this resource
    *
    * @see org.rhq.core.pluginapi.inventory.ResourceComponent#getAvailability()
    */
   public AvailabilityType getAvailability() {
      boolean trace = log.isTraceEnabled();
      EmsConnection conn = getConnection();
      try {
         conn.refresh();
         EmsBean bean = queryCacheBean();
         if (bean != null && bean.getAttribute("CacheStatus").getValue().equals(ComponentStatus.RUNNING.toString())) {
            bean.refreshAttributes();
            if (trace) log.tracef("Cache %s within %s cache manager is running and attributes could be refreshed, so it's up.", cacheName, cacheManagerName);
            return AvailabilityType.UP;
         }
         if (trace) log.tracef("Cache status for %s within %s cache manager is anything other than running, so it's down.", cacheName, cacheManagerName);
         return AvailabilityType.DOWN;
      } catch (Exception e) {
         if (trace) log.trace("There was an exception checking availability, so cache status is down.", e);
         return AvailabilityType.DOWN;
      }
   }

   /**
    * Start the resource connection
    */
   public void start(ResourceContext<CacheManagerComponent> context) {
      this.context = context;
      this.cacheManagerName = context.getParentResourceComponent().context.getResourceKey();
      this.cacheName = context.getResourceKey();
      if (log.isTraceEnabled())
         log.tracef("Start cache component for cache manager %s with cache key %s", cacheManagerName, cacheName);
   }

   /**
    * Tear down the rescource connection
    *
    * @see org.rhq.core.pluginapi.inventory.ResourceComponent#stop()
    */
   public void stop() {
   }

   /**
    * Gather measurement data
    *
    * @see org.rhq.core.pluginapi.measurement.MeasurementFacet#getValues(org.rhq.core.domain.measurement.MeasurementReport,
    *      java.util.Set)
    */
   public void getValues(MeasurementReport report, Set<MeasurementScheduleRequest> metrics) {
      boolean trace = log.isTraceEnabled();
      if (trace) log.trace("Get values metrics");
      for (MeasurementScheduleRequest req : metrics) {
         if (trace) log.tracef("Inspect metric %s", req);
         String metric = req.getName();
         try {
            EmsBean bean = queryComponentBean(metric);
            if (bean != null) {
               if (trace) log.tracef("Retrieved mbean with name %s", bean.getBeanName());
               bean.refreshAttributes();
               String attName = metric.substring(metric.indexOf(".") + 1);
               EmsAttribute att = bean.getAttribute(attName);
               // Attribute values are of various data types
               if (att != null) {
                  Object o = att.getValue();
                  Class attrType = att.getTypeClass();
                  DataType type = req.getDataType();
                  if (type == DataType.MEASUREMENT) {
                     if (o != null) {
                        MeasurementDataNumeric res = constructNumericMeasure(attrType, o, req);
                        if (res != null) report.addData(res);
                     } else {
                        if (log.isDebugEnabled()) log.debugf("Metric (%s) has null value, do not add to report", req.getName());
                     }
                  } else if (type == DataType.TRAIT) {
                     String value = (String) o;
                     if (trace) log.tracef("Metric (%s) is trait with value %s", req.getName(), value);
                     MeasurementDataTrait res = new MeasurementDataTrait(req, value);
                     report.addData(res);
                  }
               } else {
                  log.attributeNotFound(attName);
               }
            }
         }
         catch (Exception e) {
            log.getValuesFailed(metric, e);
         }
      }
   }

   /**
    * Invoke operations on the Cache MBean instance
    *
    * @param fullOpName       Name of the operation
    * @param parameters       Parameters of the Operation
    * @return OperationResult object if successful
    * @throws Exception       If operation was not successful
    */
   public OperationResult invokeOperation(String fullOpName, Configuration parameters) throws Exception {
      boolean trace = log.isTraceEnabled();
      EmsBean bean = queryComponentBean(fullOpName);
      String opName = fullOpName.substring(fullOpName.indexOf(".") + 1);
      EmsOperation ops = bean.getOperation(opName);
      Collection<PropertySimple> simples = parameters.getSimpleProperties().values();
      if (trace) log.tracef("Parameters, as simple properties, are %s", simples);
      Object[] realParams = new Object[simples.size()];
      int i = 0;
      for (PropertySimple property : simples) {
         // Since parameters are typed in UI, passing them as Strings is the only reasonable way of dealing with this
         realParams[i++] = property.getStringValue();
      }

      if (ops == null)
         throw new Exception("Operation " + fullOpName + " can't be found");
      
      Object result = ops.invoke(realParams);
      if (trace) log.tracef("Returning operation result containing %s", result.toString());
      return new OperationResult(result.toString());
   }

   private EmsConnection getConnection() {
      return context.getParentResourceComponent().getEmsConnection();
   }

   private String getSingleComponentPattern(String cacheManagerName, String cacheName, String componentName) {
      return namedCacheComponentPattern(cacheManagerName, cacheName, componentName) + ",*";
   }

   private String namedCacheComponentPattern(String cacheManagerName, String cacheName, String componentName) {
      return CacheDiscovery.cacheComponentPattern(cacheManagerName, componentName)
            + ",name=" + cacheName;
   }

   private EmsBean queryCacheBean() {
      return queryBean("Cache");
   }

   private EmsBean queryComponentBean(String name) {
      String componentName = name.substring(0, name.indexOf("."));
      return queryBean(componentName);
   }

   private EmsBean queryBean(String componentName) {
      EmsConnection conn = getConnection();
      String pattern = getSingleComponentPattern(cacheManagerName, cacheName, componentName);
      if (log.isTraceEnabled()) log.tracef("Pattern to query is %s", pattern);
      ObjectNameQueryUtility queryUtility = new ObjectNameQueryUtility(pattern);
      List<EmsBean> beans = conn.queryBeans(queryUtility.getTranslatedQuery());
      if (beans.size() > 1) {
         // If more than one are returned, most likely is due to duplicate domains which is not the general case
         log.moreThanOneBeanReturned(pattern, beans);
      }
      EmsBean bean = beans.get(0);
      if (bean == null) {
         if (log.isTraceEnabled()) log.tracef("No mbean found with name %s", pattern);
      }
      return bean;
   }
}
