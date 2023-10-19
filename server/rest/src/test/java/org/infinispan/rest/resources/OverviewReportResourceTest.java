package org.infinispan.rest.resources;

import static java.util.Map.entry;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.CompletionStage;

import org.infinispan.client.rest.RestResponse;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.rest.assertion.ResponseAssertion;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "rest.OverviewReportResourceTest")
public class OverviewReportResourceTest extends AbstractRestResourceTest {

   @Override
   public Object[] factory() {
      return new Object[]{
            new OverviewReportResourceTest().withSecurity(false),
            new OverviewReportResourceTest().withSecurity(true)
      };
   }

   @Test
   public void overviewReport() {
      CompletionStage<RestResponse> response = adminClient.server().overviewReport();
      ResponseAssertion.assertThat(response).isOk();
      Json report = Json.read(join(response).body());

      assertThat(report.at("version").asString()).isNotBlank();
      assertThat(report.at("node-id").asString()).isNotBlank();
      assertThat(report.at("coordinator-id").asString()).isNotBlank();
      assertThat(report.at("cluster-name").asString()).isNotBlank();
      assertThat(report.at("cluster-size").asInteger()).isEqualTo(2);
      assertThat(report.at("number-of-caches").asInteger()).isEqualTo(2);
      assertThat(report.at("cache-features").asJsonMap())
            .containsExactly(entry("indexed", Json.make(1)), entry("no-features", Json.make(1)));
   }
}
