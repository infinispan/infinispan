package org.infinispan.rhq;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.rhq.core.domain.measurement.MeasurementDataNumeric;
import org.rhq.core.domain.measurement.MeasurementScheduleRequest;

/**
 * RHQ utility methods
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
public class RhqUtil {

   private static final Log log = LogFactory.getLog(RhqUtil.class);

   public static MeasurementDataNumeric constructNumericMeasure(
         Class<?> attrType, Object o, MeasurementScheduleRequest req) {
      if (log.isTraceEnabled())
         log.trace("Metric ("+req.getName() +") is measurement with value " + o);
      return new MeasurementDataNumeric(req, constructDouble(attrType, o));
   }

   public static Double constructDouble(Class<?> type, Object o) {
      if (type.equals(Long.class) || type.equals(long.class))
         return Double.valueOf((Long) o);
      else if (type.equals(Double.class) || type.equals(double.class))
         return (Double) o;
      else if (type.equals(Integer.class) || type.equals(int.class))
         return Double.valueOf((Integer) o);
      else if (type.equals(String.class))
         return Double.valueOf((String) o);

      throw new IllegalStateException(String.format("Expected a value that can be converted into a double: type=%s, value=%s", type, o));
   }
}
