package org.infinispan.server.rhq;

import java.util.Set;

import org.rhq.core.domain.measurement.MeasurementReport;
import org.rhq.core.domain.measurement.MeasurementScheduleRequest;
import org.rhq.core.pluginapi.measurement.MeasurementFacet;

public class IspnServerConnector extends MetricsRemappingComponent<IspnServerConnector> implements MeasurementFacet {
   @Override
   public void getValues(MeasurementReport report, Set<MeasurementScheduleRequest> reqs) throws Exception {
      super.getValues(report, reqs);
   }
}
