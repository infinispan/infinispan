package org.infinispan.server.test.core;

import static javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag.REQUIRED;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;
import static org.infinispan.functional.FunctionalTestUtils.await;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
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
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.test.TestingUtil;
import org.wildfly.security.http.HttpConstants;
import org.wildfly.security.sasl.util.SaslMechanismInformation;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class Common {
   private static final boolean IS_IBM = System.getProperty("java.vendor").contains("IBM");

   public static final Map<String, User> USER_MAP;

   public static final Collection<Object[]> SASL_MECHS;

   public static final Collection<Object[]> SASL_KERBEROS_MECHS;

   public static final Collection<Object[]> HTTP_MECHS;

   public static final Collection<Object[]> HTTP_KERBEROS_MECHS;

   public static final Collection<Protocol> HTTP_PROTOCOLS = Arrays.asList(Protocol.values());

   static {
      USER_MAP = new HashMap<>();
      USER_MAP.put("admin", new User("admin", "adminPassword", AuthorizationPermission.ALL.name()));
      USER_MAP.put("supervisor", new User("supervisorPassword", AuthorizationPermission.ALL_READ.name(), AuthorizationPermission.ALL_WRITE.name()));
      USER_MAP.put("reader", new User("reader", "readerPassword", AuthorizationPermission.ALL_READ.name()));
      USER_MAP.put("writer", new User("writer", "writerPassword", AuthorizationPermission.ALL_WRITE.name()));
      USER_MAP.put("unprivileged", new User("unprivileged", "unprivilegedPassword", AuthorizationPermission.NONE.name()));
      USER_MAP.put("executor", new User("executor", "executorPassword", AuthorizationPermission.EXEC.name()));

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

   public static RestResponse awaitStatus(Supplier<CompletionStage<RestResponse>> request, int pendingStatus, int completeStatus) {
      int count = 0;
      RestResponse response;
      while ((response = await(request.get())).getStatus() == pendingStatus || count++ < 100) {
         response.close();
         TestingUtil.sleepThread(10);
      }
      assertEquals(completeStatus, response.getStatus());
      return response;
   }

   public static class User {
      final String username;
      final char[] password;
      final Iterable<String> groups;

      public User(String username, String password, String... groups) {
         this.username = username;
         this.password = password.toCharArray();
         this.groups = Arrays.asList(groups);
      }
   }

   public static <T> T sync(CompletionStage<T> stage) {
      return Exceptions.unchecked(() -> stage.toCompletableFuture().get(5, TimeUnit.SECONDS));
   }

   public static <T> T sync(CompletionStage<T> stage, long timeout, TimeUnit timeUnit) {
      return Exceptions.unchecked(() -> stage.toCompletableFuture().get(timeout, timeUnit));
   }

   public static void assertStatus(int status, CompletionStage<RestResponse> request) {
      try (RestResponse response = sync(request)) {
         assertEquals(status, response.getStatus());
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
