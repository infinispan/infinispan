package org.infinispan.server.insights.report;

import java.util.function.Supplier;

import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.server.insights.InsightsModule;

import com.fasterxml.jackson.databind.JsonSerializer;
import com.redhat.insights.reports.InsightsSubreport;

public class InfinispanSubreport implements InsightsSubreport {

   private final Supplier<Json> reportSupplier;
   Json jsonReport;

   public InfinispanSubreport(Supplier<Json> reportSupplier) {
      this.reportSupplier = reportSupplier;
   }

   @Override
   public void generateReport() {
      jsonReport = reportSupplier.get();
   }

   @Override
   public String getVersion() {
      return InsightsModule.REPORT_VERSION;
   }

   @Override
   public JsonSerializer<InsightsSubreport> getSerializer() {
      return new InfinispanSubReportSerializer();
   }
}
