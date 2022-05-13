package org.infinispan.server.functional;

import static org.infinispan.rest.assertion.ResponseAssertion.assertThat;
import static org.infinispan.server.test.core.Common.HTTP_PROTOCOLS;
import static org.junit.Assert.assertTrue;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.configuration.Protocol;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.rest.resources.WeakSSEListener;
import org.infinispan.server.test.junit4.InfinispanServerRule;
import org.infinispan.server.test.junit4.InfinispanServerTestMethodRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.testcontainers.shaded.org.yaml.snakeyaml.Yaml;

/**
 * Listen the container endpoint and test different serializations.
 *
 * @since 14.0
 */
@RunWith(Parameterized.class)
public class RestContainerListenerTest {

   @ClassRule
   public static InfinispanServerRule SERVERS = ClusteredIT.SERVERS;
   private final Protocol protocol;
   private final AcceptSerialization serialization;

   @Rule
   public InfinispanServerTestMethodRule SERVER_TEST = new InfinispanServerTestMethodRule(SERVERS);

   @Parameterized.Parameters(name = "{0}-{1}")
   public static Collection<Object[]> data() {
      List<Object[]> params = new ArrayList<>(HTTP_PROTOCOLS.size());
      for (Protocol protocol : HTTP_PROTOCOLS) {
         for (AcceptSerialization serialization : AcceptSerialization.values()) {
            params.add(new Object[]{protocol, serialization});
         }
      }
      return params;
   }

   public RestContainerListenerTest(Protocol protocol, AcceptSerialization serialization) {
      this.protocol = protocol;
      this.serialization = serialization;
   }

   @Test
   public void testSSECluster() throws Exception {
      RestClientConfigurationBuilder builder = new RestClientConfigurationBuilder();
      builder.protocol(protocol);
      RestClient client = SERVER_TEST.rest().withClientConfiguration(builder).create();
      WeakSSEListener sseListener = new WeakSSEListener();
      Map<String, String> headers = Collections.singletonMap("Accept", serialization.header());

      try (Closeable ignored = client.raw().listen("/rest/v2/container?action=listen", headers, sseListener)) {
         assertTrue(sseListener.await(10, TimeUnit.SECONDS));

         assertThat(client.cache("caching-listen").createWithTemplate("org.infinispan.DIST_SYNC")).isOk();

         sseListener.expectEvent("create-cache", "caching-listen");
         sseListener.expectEvent("lifecycle-event", "ISPN100002", pair -> {
            assertTrue("Not a " + serialization.header() + ": " + pair.getValue(), serialization.isAccepted(pair.getValue()));
         });
         sseListener.expectEvent("lifecycle-event", "ISPN100010", pair -> {
            assertTrue("Not a " + serialization.header() + ": " + pair.getValue(), serialization.isAccepted(pair.getValue()));
         });

         assertThat(client.cache("caching-listen").delete()).isOk();
         sseListener.expectEvent("remove-cache", "caching-listen");
      }
   }

   private enum AcceptSerialization {
      JSON {
         @Override
         public String header() {
            return MediaType.APPLICATION_JSON_TYPE;
         }

         @Override
         public boolean isAccepted(String s) {
            Json json = Json.read(s);
            if (!(json.isObject() && json.has("log"))) return false;

            Json log = json.at("log");
            if (!(log.has("content") && log.has("meta") && log.has("category"))) return false;

            Json content = log.at("content");
            Json meta = log.at("meta");
            return content.has("level") && content.has("message") && content.has("detail")
                  && meta.has("context") && meta.has("scope") && meta.has("who") && meta.has("instant");
         }
      },
      YAML {
         @Override
         public String header() {
            return MediaType.APPLICATION_YAML_TYPE;
         }

         @Override
         @SuppressWarnings("unchecked")
         public boolean isAccepted(String s) {
            Yaml yaml = new Yaml();
            LinkedHashMap<String, LinkedHashMap> ev = yaml.load(s);
            if (!(ev != null && ev.containsKey("log"))) return false;

            LinkedHashMap<String, LinkedHashMap> log = ev.get("log");
            if (!(log.containsKey("content") && log.containsKey("meta") && log.containsKey("category"))) return false;

            LinkedHashMap<String, String> content = log.get("content");
            LinkedHashMap<String, String> meta = log.get("meta");
            return content.containsKey("level") && content.containsKey("message") && content.containsKey("detail")
                  && meta.containsKey("context") && meta.containsKey("scope") && meta.containsKey("who") && meta.containsKey("instant");
         }
      },
      XML {
         @Override
         public String header() {
            return MediaType.APPLICATION_XML_TYPE;
         }

         @Override
         public boolean isAccepted(String content) {
            return content.startsWith("<?xml version=\"1.0\"?>\n<log category=")
                  && content.endsWith("</log>");
         }
      };

      public abstract String header();

      public abstract boolean isAccepted(String content);
   }
}
