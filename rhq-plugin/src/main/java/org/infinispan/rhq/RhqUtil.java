package org.infinispan.rhq;

import org.infinispan.rhq.logging.Log;
import org.infinispan.util.Util;
import org.infinispan.util.logging.LogFactory;
import org.rhq.core.domain.measurement.MeasurementDataNumeric;
import org.rhq.core.domain.measurement.MeasurementScheduleRequest;

/**
 * RHQ utility methods
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
public class RhqUtil {

   private static final Log log = LogFactory.getLog(RhqUtil.class, Log.class);

   public static MeasurementDataNumeric constructNumericMeasure(
         Class attrType, Object o, MeasurementScheduleRequest req) {
      if (log.isTraceEnabled())
         log.tracef("Metric (%s) is measurement with value %s", req.getName(), o);
      return new MeasurementDataNumeric(req, Util.constructDouble(attrType, o));
   }

}
