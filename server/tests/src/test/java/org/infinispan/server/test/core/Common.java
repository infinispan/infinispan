package org.infinispan.server.test.core;

import static javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag.REQUIRED;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import javax.security.auth.Subject;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;

import org.apache.logging.log4j.core.util.StringBuilderWriter;
import org.infinispan.client.hotrod.security.BasicCallbackHandler;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.configuration.Protocol;
import org.infinispan.commons.configuration.io.ConfigurationWriter;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.test.TestingUtil;
import org.wildfly.security.http.HttpConstants;
import org.wildfly.security.sasl.util.SaslMechanismInformation;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class Common {
   private static final boolean IS_IBM = System.getProperty("java.vendor").contains("IBM");

   public static final Collection<Object[]> SASL_MECHS;

   public static final Collection<Object[]> SASL_KERBEROS_MECHS;

   public static final Collection<Object[]> HTTP_MECHS;

   public static final Collection<Object[]> HTTP_KERBEROS_MECHS;

   public static final Collection<Protocol> HTTP_PROTOCOLS = Arrays.asList(Protocol.values());

   public static final String[] NASHORN_DEPS = new String[]{
         "org.openjdk.nashorn:nashorn-core:15.3",
         "org.ow2.asm:asm:7.3.1",
         "org.ow2.asm:asm-util:7.3.1"
   };

   static {
      SASL_MECHS = new ArrayList<>();
      SASL_MECHS.add(new Object[]{""});
      SASL_MECHS.add(new Object[]{SaslMechanismInformation.Names.PLAIN});

      SASL_MECHS.add(new Object[]{SaslMechanismInformation.Names.DIGEST_MD5});
      SASL_MECHS.add(new Object[]{SaslMechanismInformation.Names.DIGEST_SHA_512});
      SASL_MECHS.add(new Object[]{SaslMechanismInformation.Names.DIGEST_SHA_384});
      SASL_MECHS.add(new Object[]{SaslMechanismInformation.Names.DIGEST_SHA_256});
      SASL_MECHS.add(new Object[]{SaslMechanismInformation.Names.DIGEST_SHA});

      SASL_MECHS.add(new Object[]{SaslMechanismInformation.Names.SCRAM_SHA_512});
      SASL_MECHS.add(new Object[]{SaslMechanismInformation.Names.SCRAM_SHA_384});
      SASL_MECHS.add(new Object[]{SaslMechanismInformation.Names.SCRAM_SHA_256});
      SASL_MECHS.add(new Object[]{SaslMechanismInformation.Names.SCRAM_SHA_1});

      SASL_KERBEROS_MECHS = new ArrayList<>();
      SASL_KERBEROS_MECHS.add(new Object[]{""});
      SASL_KERBEROS_MECHS.add(new Object[]{SaslMechanismInformation.Names.GSSAPI});
      SASL_KERBEROS_MECHS.add(new Object[]{SaslMechanismInformation.Names.GS2_KRB5});

      HTTP_MECHS = new ArrayList<>();
      HTTP_MECHS.add(new Object[]{""});
      HTTP_MECHS.add(new Object[]{HttpConstants.BASIC_NAME});
      HTTP_MECHS.add(new Object[]{HttpConstants.DIGEST_NAME});

      HTTP_KERBEROS_MECHS = new ArrayList<>();
      HTTP_KERBEROS_MECHS.add(new Object[]{""});
      HTTP_KERBEROS_MECHS.add(new Object[]{HttpConstants.SPNEGO_NAME});
   }

   public static <T> T awaitStatus(Supplier<CompletionStage<RestResponse>> request, int pendingStatus, int completeStatus, Function<RestResponse, T> f) {
      int MAX_RETRIES = 100;
      for (int i = 0; i < MAX_RETRIES; i++) {
         try (RestResponse response = sync(request.get())) {
            if (response.getStatus() == pendingStatus) {
               TestingUtil.sleepThread(100);
            } else if (response.getStatus() == completeStatus) {
               return f.apply(response);
            } else {
               fail(String.format("Request returned unexpected status %d instead of %d or %d", response.getStatus(), pendingStatus, completeStatus));
            }
         }
      }
      fail(String.format("Request did not complete with status %d after %d retries", completeStatus, MAX_RETRIES));
      return null; // Never executed
   }

   public static void awaitStatus(Supplier<CompletionStage<RestResponse>> request, int pendingStatus, int completeStatus) {
      awaitStatus(request, pendingStatus, completeStatus, r -> null);
   }

   public static RestResponse awaitResponse(Supplier<CompletionStage<RestResponse>> request, int pendingStatus, int completeStatus) {
      int MAX_RETRIES = 100;
      for (int i = 0; i < MAX_RETRIES; i++) {
         RestResponse response = sync(request.get());
         if (response.getStatus() == pendingStatus) {
            response.close();
            TestingUtil.sleepThread(100);
         } else if (response.getStatus() == completeStatus) {
            return response;
         } else {
            response.close();
            fail(String.format("Request returned unexpected status %d instead of %d or %d", response.getStatus(), pendingStatus, completeStatus));
         }
      }
      fail(String.format("Request did not complete with status %d after %d retries", completeStatus, MAX_RETRIES));
      return null; // Never executed
   }

   public static <T> T sync(CompletionStage<T> stage) {
      return sync(stage, 10, TimeUnit.SECONDS);
   }

   public static <T> T sync(CompletionStage<T> stage, long timeout, TimeUnit timeUnit) {
      return Exceptions.unchecked(() -> stage.toCompletableFuture().get(timeout, timeUnit));
   }

   public static String assertStatus(int status, CompletionStage<RestResponse> request) {
      try (RestResponse response = sync(request)) {
         assertEquals(status, response.getStatus());
         return response.getBody();
      }
   }

   public static void assertStatusAndBodyEquals(int status, String body, CompletionStage<RestResponse> response) {
      assertEquals(body, assertStatus(status, response));
   }

   public static void assertStatusAndBodyContains(int status, String body, CompletionStage<RestResponse> response) {
      String responseBody = assertStatus(status, response);
      assertTrue(responseBody, responseBody.contains(body));
   }

   public static void assertResponse(int status, CompletionStage<RestResponse> request, Consumer<RestResponse> consumer) {
      try (RestResponse response = sync(request)) {
         assertEquals(status, response.getStatus());
         consumer.accept(response);
      }
   }

   public static Subject createSubject(String principal, String realm, char[] password) {
      return Exceptions.unchecked(() -> {
         LoginContext context = new LoginContext("KDC", null, new BasicCallbackHandler(principal, realm, password), createJaasConfiguration(false));
         context.login();
         return context.getSubject();
      });
   }

   private static Configuration createJaasConfiguration(boolean server) {
      return new Configuration() {
         @Override
         public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
            if (!"KDC".equals(name)) {
               throw new IllegalArgumentException(String.format("Unexpected name '%s'", name));
            }

            AppConfigurationEntry[] entries = new AppConfigurationEntry[1];
            Map<String, Object> options = new HashMap<>();
            //options.put("debug", "true");
            options.put("refreshKrb5Config", "true");

            if (IS_IBM) {
               options.put("noAddress", "true");
               options.put("credsType", server ? "acceptor" : "initiator");
               entries[0] = new AppConfigurationEntry("com.ibm.security.auth.module.Krb5LoginModule", REQUIRED, options);
            } else {
               options.put("storeKey", "true");
               options.put("isInitiator", server ? "false" : "true");
               entries[0] = new AppConfigurationEntry("com.sun.security.auth.module.Krb5LoginModule", REQUIRED, options);
            }
            return entries;
         }

      };
   }

   public static String cacheConfigToJson(String name, org.infinispan.configuration.cache.Configuration configuration) {
      StringBuilderWriter sw = new StringBuilderWriter();
      try (ConfigurationWriter w = ConfigurationWriter.to(sw).withType(APPLICATION_JSON).prettyPrint(false).build()) {
         new ParserRegistry().serialize(w, name, configuration);
      }
      return sw.toString();
   }

}
