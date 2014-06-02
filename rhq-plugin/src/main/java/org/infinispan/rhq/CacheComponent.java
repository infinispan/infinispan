package org.infinispan.rhq;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.mc4j.ems.connection.EmsConnection;
import org.mc4j.ems.connection.bean.EmsBean;
import org.mc4j.ems.connection.bean.EmsBeanName;
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
import org.rhq.plugins.jmx.util.ObjectNameQueryUtility;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import static org.infinispan.rhq.RhqUtil.constructNumericMeasure;

/**
 * Component class for Caches within Infinispan
 *
 * @author Heiko W. Rupp
 * @author Galder Zamarreño
 * @author Tristan Tarrant
 */
public class CacheComponent extends MBeanResourceComponent<CacheManagerComponent> {
   private static final Log log = LogFactory.getLog(CacheComponent.class);

   private String cacheManagerName;
   private String cacheName;

   /**
    * Start the resource connection
    */
   @Override
   public void start(ResourceContext<CacheManagerComponent> context) {
      this.cacheManagerName = context.getParentResourceComponent().getResourceContext().getResourceKey();
      this.cacheName = context.getResourceKey();
      if (log.isTraceEnabled())
         log.trace("Start cache component for cache manager "+cacheManagerName+" with cache key "+cacheName);
      super.start(context);
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
      if (trace) log.trace("Get values metrics");
      EmsConnection conn = getEmsConnection();
      // First query them all so we don't have to do find them individually
      conn.queryBeans(namedCacheComponentPattern(cacheManagerName, cacheName, "*"));
      for (MeasurementScheduleRequest req : metrics) {
         if (trace) log.trace("Inspect metric " + req);
         String metric = req.getName();
         try {
            String metricBeanName = namedCacheComponentPattern(cacheManagerName, cacheName,
                                                               metric.substring(0, metric.indexOf(".")));
            EmsBean bean = conn.getBean(metricBeanName);
            if (bean != null) {
               if (trace) log.trace("Retrieved mbean with name "+ bean.getBeanName());
               bean.refreshAttributes();
               String attName = metric.substring(metric.indexOf(".") + 1);
               EmsAttribute att = bean.getAttribute(attName);
               // Attribute values are of various data types
               if (att != null) {
                  Object o = att.getValue();
                  Class<?> attrType = att.getTypeClass();
                  DataType type = req.getDataType();
                  if (type == DataType.MEASUREMENT) {
                     if (o != null) {
                        MeasurementDataNumeric res = constructNumericMeasure(attrType, o, req);
                        if (res != null) report.addData(res);
                     } else {
                        if (log.isDebugEnabled()) log.debug("Metric ("+req.getName()+") has null value, do not add to report");
                     }
                  } else if (type == DataType.TRAIT) {
                     String value = (String) o;
                     if (trace) log.trace("Metric ("+req.getName()+") is trait with value " + value);
                     MeasurementDataTrait res = new MeasurementDataTrait(req, value);
                     report.addData(res);
                  }
               } else {
                  if(log.isWarnEnabled()) {
                     log.warn("Attribute "+attName+" not found");
                  }
               }
            }
         }
         catch (Exception e) {
            if(log.isWarnEnabled()) {
               log.warn("getValues failed for "+metric, e);
            }
         }
      }
   }

   /**
    * Invoke operations on the Cache MBean instance
    *
    * @param fullName       Name of the operation
    * @param parameters       Parameters of the Operation
    * @return OperationResult object if successful
    * @throws Exception       If operation was not successful
    */
   @Override
   public OperationResult invokeOperation(String fullName, Configuration parameters) throws Exception {
      boolean trace = log.isTraceEnabled();
      int paramSep = fullName.indexOf('|');
      String fullOpName = paramSep < 0 ? fullName : fullName.substring(0, paramSep);
      EmsBean bean = queryComponentBean(getConnection(), fullOpName);
      String opName = fullOpName.substring(fullOpName.indexOf(".") + 1);
      EmsOperation ops = bean.getOperation(opName);
      Collection<PropertySimple> simples = parameters.getSimpleProperties().values();
      if (trace) log.trace("Parameters, as simple properties, are " + simples);
      Object[] realParams = new Object[simples.size()];
      int i = 0;
      for (PropertySimple property : simples) {
         // Since parameters are typed in UI, passing them as Strings is the only reasonable way of dealing with this
         realParams[i++] = property.getStringValue();
      }

      if (ops == null)
         throw new Exception("Operation " + fullOpName + " can't be found");

      Object result = ops.invoke(realParams);
      String sResult = result != null ? result.toString() : "";
      if (trace)
         log.trace("Returning operation result containing " + sResult);
      return new OperationResult(sResult);
   }

   private EmsConnection getConnection() {
      return getResourceContext().getParentResourceComponent().getEmsConnection();
   }

   private String getSingleComponentPattern(String cacheManagerName, String cacheName, String componentName) {
      return namedCacheComponentPattern(cacheManagerName, cacheName, componentName) + ",*";
   }

   private String namedCacheComponentPattern(String cacheManagerName, String cacheName, String componentName) {
      return CacheDiscovery.cacheComponentPattern(cacheManagerName, componentName)
            + ",name=" + cacheName;
   }

   private EmsBean queryComponentBean(EmsConnection conn, String name) {
      String componentName = name.substring(0, name.indexOf("."));
      return queryBean(conn, componentName);
   }

   private EmsBean queryBean(EmsConnection conn, String componentName) {
      String pattern = getSingleComponentPattern(cacheManagerName, cacheName, componentName);
      if (log.isTraceEnabled()) log.trace("Pattern to query is " + pattern);
      ObjectNameQueryUtility queryUtility = new ObjectNameQueryUtility(pattern);
      List<EmsBean> beans = conn.queryBeans(queryUtility.getTranslatedQuery());
      for (EmsBean bean : beans) {
         if (isCacheComponent(bean, componentName)) {
            return bean;
         } else {
            log.warn(String.format("MBeanServer returned spurious object %s", bean.getBeanName().getCanonicalName()));
         }
      }
      if (log.isTraceEnabled()) log.trace("No mbean found with name " + pattern);
      return null;
   }

   protected static boolean isCacheComponent(EmsBean bean, String componentName) {
      EmsBeanName beanName = bean.getBeanName();
      return "Cache".equals(beanName.getKeyProperty("type")) && componentName.equals(beanName.getKeyProperty("component"));
   }

   @Override
   protected EmsBean loadBean() {
      return getEmsConnection().getBean(namedCacheComponentPattern(cacheManagerName, cacheName, "Cache"));
   }
}
