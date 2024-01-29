package org.infinispan.server.insights.report;

import java.io.IOException;

import com.redhat.insights.reports.InsightsSubreport;
import org.infinispan.server.insights.helper.JsonGeneratorHelper;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

public class InfinispanSubReportSerializer extends JsonSerializer<InsightsSubreport> {

   @Override
   public void serialize(InsightsSubreport insightsSubreport, JsonGenerator generator, SerializerProvider serializerProvider) throws IOException {
      InfinispanSubreport report = (InfinispanSubreport) insightsSubreport;
      JsonGeneratorHelper.write(report.jsonReport, generator);
      generator.flush();
   }
}
