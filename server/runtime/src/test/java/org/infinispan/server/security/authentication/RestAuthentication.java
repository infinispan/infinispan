package org.infinispan.server.security.authentication;

import static org.infinispan.server.security.Common.sync;
import static org.junit.Assert.assertEquals;

import java.util.Collection;

import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.commons.test.ThreadLeakChecker;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.server.security.Common;
import org.infinispan.server.test.InfinispanServerRule;
import org.infinispan.server.test.InfinispanServerTestMethodRule;
import org.infinispan.server.test.category.Security;
import org.junit.AfterClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/

@RunWith(Parameterized.class)
@Category(Security.class)
public class RestAuthentication {

   @ClassRule
   public static InfinispanServerRule SERVERS = AuthenticationIT.SERVERS;

   @Rule
   public InfinispanServerTestMethodRule SERVER_TEST = new InfinispanServerTestMethodRule(SERVERS);

   private final String mechanism;

   @Parameterized.Parameters(name = "{0}")
   public static Collection<Object[]> data() {
      return Common.HTTP_MECHS;
   }

   public RestAuthentication(String mechanism) {
      this.mechanism = mechanism;
   }

   @Test
   public void testRestReadWrite() {
      RestClientConfigurationBuilder builder = new RestClientConfigurationBuilder();
      if (!mechanism.isEmpty()) {
         builder.security().authentication()
               .mechanism(mechanism)
               .realm("default")
               .username("all_user")
               .password("all");
      }
      RestClient client = SERVER_TEST.getRestClient(builder, CacheMode.DIST_SYNC);
      RestResponse response = sync(client.post(SERVER_TEST.getMethodName(), "k1", "v1"));
      if (mechanism.isEmpty()) {
         assertEquals(401, response.getStatus());
      } else {
         assertEquals(200, response.getStatus());
         response = sync(client.get(SERVER_TEST.getMethodName(), "k1"));
         assertEquals(200, response.getStatus());
         assertEquals("v1", response.getBody());
      }
   }

   @AfterClass
   public static void afterClass() {
      // https://issues.jboss.org/browse/ELY-1843
      ThreadLeakChecker.ignoreThreadsContaining("pool-.*");
   }
}
