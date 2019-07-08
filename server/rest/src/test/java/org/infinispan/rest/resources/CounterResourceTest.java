package org.infinispan.rest.resources;

import static org.testng.AssertJUnit.assertEquals;

import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.util.StringContentProvider;
import org.eclipse.jetty.http.HttpMethod;
import org.infinispan.counter.EmbeddedCounterManagerFactory;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterManager;
import org.infinispan.counter.api.CounterType;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.rest.assertion.ResponseAssertion;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "rest.CounterResourceTest")
public class CounterResourceTest extends AbstractRestResourceTest {

   @Override
   protected void defineCaches(EmbeddedCacheManager cm) {
      CounterManager counterManager = EmbeddedCounterManagerFactory.asCounterManager(cm);
      counterManager.defineCounter("weak", CounterConfiguration.builder(CounterType.WEAK).build());
      counterManager.defineCounter("strong", CounterConfiguration.builder(CounterType.UNBOUNDED_STRONG).build());
   }

   @Test
   public void testWeakCounter() throws Exception {
      String url = String.format("http://localhost:%d/rest/v2/counters/weak", restServer().getPort());
      ContentResponse response = client.newRequest(url)
            .method(HttpMethod.POST).content(new StringContentProvider("10")).send();
      ResponseAssertion.assertThat(response).isOk();
      eventually(() -> {
         ContentResponse r = client.newRequest(url)
               .method(HttpMethod.GET).send();
         ResponseAssertion.assertThat(r).isOk();
         long value = Long.parseLong(r.getContentAsString());
         return value == 10;
      });
      response = client.newRequest(url)
            .method(HttpMethod.DELETE).send();
      ResponseAssertion.assertThat(response).isOk();
      response = client.newRequest(url)
            .method(HttpMethod.GET).send();
      ResponseAssertion.assertThat(response).isOk();
      assertEquals(0, Long.parseLong(response.getContentAsString()));
   }

   @Test
   public void testStrongCounter() throws Exception {
      String url = String.format("http://localhost:%d/rest/v2/counters/strong", restServer().getPort());
      ContentResponse response = client.newRequest(url)
            .method(HttpMethod.POST).content(new StringContentProvider("10")).send();
      ResponseAssertion.assertThat(response).isOk();
      assertEquals(10, Long.parseLong(response.getContentAsString()));
      response = client.newRequest(url)
            .method(HttpMethod.GET).send();
      ResponseAssertion.assertThat(response).isOk();
      assertEquals(10, Long.parseLong(response.getContentAsString()));
      response = client.newRequest(url)
            .method(HttpMethod.DELETE).send();
      ResponseAssertion.assertThat(response).isOk();
      response = client.newRequest(url)
            .method(HttpMethod.GET).send();
      ResponseAssertion.assertThat(response).isOk();
      assertEquals(0, Long.parseLong(response.getContentAsString()));
   }
}
