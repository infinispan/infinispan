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
package org.infinispan.jopr;

import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.mc4j.ems.connection.EmsConnection;
import org.mc4j.ems.connection.bean.EmsBean;
import org.mc4j.ems.connection.bean.attribute.EmsAttribute;
import org.mc4j.ems.connection.bean.operation.EmsOperation;
import org.rhq.core.domain.configuration.Configuration;
import org.rhq.core.domain.measurement.AvailabilityType;
import org.rhq.core.domain.measurement.DataType;
import org.rhq.core.domain.measurement.MeasurementDataNumeric;
import org.rhq.core.domain.measurement.MeasurementDataTrait;
import org.rhq.core.domain.measurement.MeasurementReport;
import org.rhq.core.domain.measurement.MeasurementScheduleRequest;
import org.rhq.core.pluginapi.inventory.ResourceComponent;
import org.rhq.core.pluginapi.inventory.ResourceContext;
import org.rhq.core.pluginapi.measurement.MeasurementFacet;
import org.rhq.core.pluginapi.operation.OperationFacet;
import org.rhq.core.pluginapi.operation.OperationResult;

import java.util.Set;

/**
 * Component class for Caches within Infinispan
 *
 * @author Heiko W. Rupp
 * @author Galder Zamarreño
 */
public class CacheComponent implements ResourceComponent<CacheManagerComponent>, MeasurementFacet, OperationFacet {
   private static final Log log = LogFactory.getLog(CacheComponent.class);

   private ResourceContext<CacheManagerComponent> context;
   
   /** The naming pattern of the current bean without the actual bean name */
   private String myNamePattern;

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
         EmsBean bean = conn.getBean(context.getResourceKey());
         EmsAttribute attribute = bean.getAttribute("CacheStatus");
         if (attribute.getValue().equals(ComponentStatus.RUNNING.toString())) {
            bean.refreshAttributes();
            if (trace) log.trace("Cache status is running and attributes could be refreshed, so it's up."); 
            return AvailabilityType.UP;
         }
         if (trace) log.trace("Cache status is anything other than running, so it's down.");
         return AvailabilityType.DOWN;
      } catch (Exception e) {
         if (trace) log.trace("There was an exception checking availability, so cache status is down.");
         return AvailabilityType.DOWN;
      }
   }


   /**
    * Start the resource connection
    *
    * @see org.rhq.core.pluginapi.inventory.ResourceComponent#start(org.rhq.core.pluginapi.inventory.ResourceContext)
    */
   public void start(ResourceContext<CacheManagerComponent> context) throws Exception {
      this.context = context;
      myNamePattern = context.getResourceKey();
      myNamePattern = myNamePattern.substring(0, myNamePattern.indexOf("jmx-resource=") + 13);
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
   public void getValues(MeasurementReport report, Set<MeasurementScheduleRequest> metrics) throws Exception {
      boolean trace = log.isTraceEnabled();
      if (trace) log.trace("Get values metrics");
      EmsConnection conn = getConnection();
      for (MeasurementScheduleRequest req : metrics) {
         if (trace) log.trace("Inspect metric {0}", req);
         String metric = req.getName();
         try {
            String abbrev = metric.substring(0, metric.indexOf("."));
            String mbean = myNamePattern + abbrev;
            EmsBean bean = conn.getBean(mbean);
            if (bean != null) {
               if (trace) log.trace("Retrieved mbean with name {0}", mbean);
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
                        MeasurementDataNumeric res = constructMeasurementDataNumeric(attrType, o, req);
                        if (res != null) report.addData(res);
                     } else {
                        if (log.isDebugEnabled()) log.debug("Metric ({0}) has null value, do not add to report", req.getName());
                     }
                  } else if (type == DataType.TRAIT) {
                     String value = (String) o;
                     if (trace) log.trace("Metric ({0}) is trait with value {1}", req.getName(), value);
                     MeasurementDataTrait res = new MeasurementDataTrait(req, value);
                     report.addData(res);
                  }
               } else {
                  log.warn("Attribute {0} not found", attName);
               }
            } else {
               if (trace) log.trace("No mbean found with name {0}", mbean);
            }
         }
         catch (Exception e) {
            log.warn("getValues failed for " + metric + " : ", e);
         }
      }
   }

   /**
    * Invoke operations on the Cache MBean instance
    *
    * @param name       Name of the operation
    * @param parameters Parameters of the Operation
    * @return OperationResult object if successful
    * @throws Exception If operation was not successful
    */
   public OperationResult invokeOperation(String name,
                                          Configuration parameters) throws Exception {
      EmsConnection conn = getConnection();
      String abbrev = name.substring(0, name.indexOf("."));
      String mbean = myNamePattern + abbrev;
      EmsBean bean = conn.getBean(mbean);
      String opName = name.substring(name.indexOf(".") + 1);
      EmsOperation ops = bean.getOperation(opName);
      if (ops != null)
         ops.invoke(new Object[]{});
      else
         throw new Exception("Operation " + name + " can't be found");


      return new OperationResult();
   }

   private EmsConnection getConnection() {
      return context.getParentResourceComponent().getConnection();
   }

   private MeasurementDataNumeric constructMeasurementDataNumeric(Class attrType, Object o, MeasurementScheduleRequest req) {
      boolean trace = log.isTraceEnabled();
      if (trace) log.trace("Metric ({0}) is measurement with value {1}", req.getName(), o);
      if (attrType.equals(Long.class) || attrType.equals(long.class)) {
         Long tmp = (Long) o;
         return new MeasurementDataNumeric(req, Double.valueOf(tmp));
      } else if (attrType.equals(Double.class) || attrType.equals(double.class)) {
         Double tmp = (Double) o;
         return new MeasurementDataNumeric(req, tmp);
      } else if (attrType.equals(Integer.class) || attrType.equals(int.class)) {
         Integer tmp = (Integer) o;
         return new MeasurementDataNumeric(req, Double.valueOf(tmp));
      } else if (attrType.equals(String.class)) {
         String tmp = (String) o;
         return new MeasurementDataNumeric(req, Double.valueOf(tmp));
      } 
      
      log.warn("Unknown {0} attribute type for {1}", attrType, o);
      return null;
   }
}