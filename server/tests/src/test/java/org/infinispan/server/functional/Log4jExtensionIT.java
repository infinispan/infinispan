package org.infinispan.server.functional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.server.test.core.Common.sync;
import static org.infinispan.server.test.core.TestSystemPropertyNames.INFINISPAN_TEST_SERVER_LOG_FILE;

import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.infinispan.server.test.junit5.InfinispanServerExtensionBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class Log4jExtensionIT {

   @RegisterExtension
   public static final InfinispanServerExtension SERVERS =
         InfinispanServerExtensionBuilder.config("configuration/ClusteredServerTest.xml")
               .env("SERVER_LIBS", "biz.paluch.logging:logstash-gelf:1.15.1")
               .property(INFINISPAN_TEST_SERVER_LOG_FILE, "log4j2-gelf.xml")
               .args("-l", "log4j2-gelf.xml")
               .numServers(1)
               .build();

   @Test
   public void testGelfAppenderDiscovery() {
      RestClient client = SERVERS.rest().get();
      RestResponse response = sync(client.server().logging().listAppenders());
      Json appenders = Json.read(response.body());
      assertThat(appenders.asMap()).containsKey("GELF");
   }
}
