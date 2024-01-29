package org.infinispan.server.insights.report;

import java.util.Map;
import java.util.function.Supplier;

import com.redhat.insights.config.InsightsConfiguration;
import com.redhat.insights.core.reports.TopLevelReportBaseImpl;
import com.redhat.insights.logging.InsightsLogger;
import com.redhat.insights.reports.InsightsSubreport;

public class InfinispanTopReport extends TopLevelReportBaseImpl {

   private final Supplier<String> identificationName;

   public InfinispanTopReport(InsightsLogger logger, InsightsConfiguration config, Supplier<String> identificationName,
                              Map<String, InsightsSubreport> subReports) {
      super(logger, config, subReports);
      this.identificationName = identificationName;
   }

   @Override
   protected String getIdentificationName() {
      return identificationName.get();
   }
}
