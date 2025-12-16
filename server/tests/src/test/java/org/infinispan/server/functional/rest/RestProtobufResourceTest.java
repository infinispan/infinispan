package org.infinispan.server.functional.rest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.test.TestingUtil.join;

import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.configuration.Protocol;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.util.Util;
import org.infinispan.server.functional.ClusteredIT;
import org.infinispan.server.test.api.TestClientDriver;
import org.infinispan.server.test.core.ResponseAssertion;
import org.infinispan.server.test.junit5.InfinispanServer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

public class RestProtobufResourceTest {

   @InfinispanServer(ClusteredIT.class)
   public static TestClientDriver SERVERS;

   @ParameterizedTest
   @EnumSource(Protocol.class)
   public void testProtobufTypes(Protocol protocol) throws Throwable {
      RestClientConfigurationBuilder builder = new RestClientConfigurationBuilder();
      builder.protocol(protocol);

      RestClient client = SERVERS.rest().withClientConfiguration(builder).create();
      String proto = Util.getResourceAsString("proto/json.proto", getClass().getClassLoader());
      ResponseAssertion.assertThat(client.schemas().put("json", proto)).isOk();

      RestResponse types = join(client.schemas().types());
      ResponseAssertion.assertThat(types).isOk();

      Json json = Json.read(types.body());
      assertThat(json.asList())
            .contains("proto.JSON")
            .hasSizeGreaterThanOrEqualTo(1);
   }
}
