package org.infinispan.server.functional.rest;

import static org.infinispan.rest.assertion.ResponseAssertion.assertThat;
import static org.infinispan.server.test.core.Common.HTTP_PROTOCOLS;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.Closeable;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.configuration.Protocol;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.rest.resources.WeakSSEListener;
import org.infinispan.server.functional.ClusteredIT;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.testcontainers.shaded.org.yaml.snakeyaml.Yaml;

/**
 * Listen the container endpoint and test different serializations.
 *
 * @since 14.0
 */
public class RestContainerListenerTest {

   @RegisterExtension
   public static InfinispanServerExtension SERVERS = ClusteredIT.SERVERS;

   static class ArgsProvider implements ArgumentsProvider {
      @Override
      public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
         return HTTP_PROTOCOLS.stream()
               .flatMap(protocol ->
                     Arrays.stream(AcceptSerialization.values())
                           .map(serialization -> Arguments.of(protocol, serialization))
               );
      }
   }

   @ParameterizedTest(name = "{0}-{1}")
   @ArgumentsSource(ArgsProvider.class)
   public void testSSECluster(Protocol protocol, AcceptSerialization serialization) throws Exception {
      RestClientConfigurationBuilder builder = new RestClientConfigurationBuilder();
      builder.protocol(protocol);
      RestClient client = SERVERS.rest().withClientConfiguration(builder).create();
      WeakSSEListener sseListener = new WeakSSEListener();
      Map<String, String> headers = Collections.singletonMap("Accept", serialization.header());

      try (Closeable ignored = client.raw().listen("/rest/v2/container?action=listen", headers, sseListener)) {
         assertTrue(sseListener.await(10, TimeUnit.SECONDS));

         assertThat(client.cache("caching-listen").createWithTemplate("org.infinispan.DIST_SYNC")).isOk();

         sseListener.expectEvent("create-cache", "caching-listen");
         sseListener.expectEvent("lifecycle-event", "ISPN100002", pair -> {
            assertTrue(serialization.isAccepted(pair.getValue()), "Not a " + serialization.header() + ": " + pair.getValue());
         });
         sseListener.expectEvent("lifecycle-event", "ISPN100010", pair -> {
            assertTrue(serialization.isAccepted(pair.getValue()), "Not a " + serialization.header() + ": " + pair.getValue());
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
            return content.startsWith("<?xml version=\"1.0\"?><log category=")
                  && content.endsWith("</log>");
         }
      };

      public abstract String header();

      public abstract boolean isAccepted(String content);
   }
}
