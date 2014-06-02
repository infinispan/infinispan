package org.infinispan.rhq;

import static org.infinispan.rhq.RhqUtil.constructNumericMeasure;

import java.util.List;
import java.util.Set;

import javax.management.ObjectName;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.mc4j.ems.connection.EmsConnection;
import org.mc4j.ems.connection.bean.EmsBean;
import org.mc4j.ems.connection.bean.EmsBeanName;
import org.mc4j.ems.connection.bean.attribute.EmsAttribute;
import org.rhq.core.domain.configuration.Configuration;
import org.rhq.core.domain.measurement.AvailabilityType;
import org.rhq.core.domain.measurement.DataType;
import org.rhq.core.domain.measurement.MeasurementDataNumeric;
import org.rhq.core.domain.measurement.MeasurementDataTrait;
import org.rhq.core.domain.measurement.MeasurementReport;
import org.rhq.core.domain.measurement.MeasurementScheduleRequest;
import org.rhq.core.pluginapi.inventory.ResourceContext;
import org.rhq.core.pluginapi.operation.OperationResult;
import org.rhq.plugins.jmx.JMXServerComponent;
import org.rhq.plugins.jmx.MBeanResourceComponent;
import org.rhq.plugins.jmx.util.ObjectNameQueryUtility;

/**
 * The component class for the Infinispan manager
 *
 * @author Heiko W. Rupp
 * @author Galder Zamarreño
 * @author Tristan Tarrant
 */
public class CacheManagerComponent extends MBeanResourceComponent<JMXServerComponent<?>> {
   private static final Log log = LogFactory.getLog(CacheManagerComponent.class);
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
         EmsBean bean = queryCacheManagerBean(conn);
         if (bean != null) {
            if (trace) log.trace("Cache manager "+bean+" could be found, so it's up.");
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
      cacheManagerPattern = context.getResourceKey() + "," + CacheManagerDiscovery.CACHE_MANAGER_JMX_GROUP;
      super.start(context);
   }

   @Override
   public EmsConnection getEmsConnection() {
      return getResourceContext().getParentResourceComponent().getEmsConnection();
   }

   @Override
   protected EmsBean loadBean() {
      return queryCacheManagerBean(getEmsConnection());
   }

   @Override
   public OperationResult invokeOperation(String name, Configuration parameters) throws Exception {
      int paramSep = name.indexOf('|');
      if (paramSep > 0) {
         return super.invokeOperation(name.substring(0, paramSep), parameters);
      } else {
         return super.invokeOperation(name, parameters);
      }
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
      EmsBean bean = getEmsBean();
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
      EmsBean bean = conn.getBean(pattern);
      if (bean != null) {
         return bean;
      }
      throw new IllegalStateException("MBeanServer unexpectedly did not return any CacheManager components");
   }

   protected static boolean isCacheManagerComponent(EmsBean bean) {
      EmsBeanName beanName = bean.getBeanName();
      return "CacheManager".equals(beanName.getKeyProperty("type")) && "CacheManager".equals(beanName.getKeyProperty("component"));
   }
}
