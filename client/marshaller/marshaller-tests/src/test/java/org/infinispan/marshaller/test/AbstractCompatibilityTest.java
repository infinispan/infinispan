package org.infinispan.marshaller.test;

import static org.testng.AssertJUnit.assertEquals;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.ByteArrayRequestEntity;
import org.apache.commons.httpclient.methods.EntityEnclosingMethod;
import org.apache.commons.httpclient.methods.PutMethod;
import org.infinispan.commons.dataconversion.Encoder;
import org.infinispan.commons.dataconversion.IdentityEncoder;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.it.compatibility.CompatibilityCacheFactory;
import org.infinispan.it.compatibility.EmbeddedRestMemcachedHotRodTest;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

/**
 * @author Ryan Emerson
 * @since 9.0
 */
@Test(groups = "functional")
public abstract class AbstractCompatibilityTest extends EmbeddedRestMemcachedHotRodTest {

   @AfterClass
   protected void teardown() {
      CompatibilityCacheFactory.killCacheFactories(cacheFactory);
   }

   protected abstract Class<? extends Encoder> getEncoderClass();

   @Test
   public void testRestPutEmbeddedMemcachedHotRodGetTest() throws Exception {
      final String key = "3";
      final Object value = "<hey>ho</hey>";
      final Marshaller marshaller = cacheFactory.getMarshaller();

      // 1. Put with REST
      byte[] bytes = marshaller.objectToByteBuffer(value);
      EntityEnclosingMethod put = new PutMethod(cacheFactory.getRestUrl() + "/" + key);
      put.setRequestEntity(new ByteArrayRequestEntity(bytes, "application/octet-stream"));
      HttpClient restClient = cacheFactory.getRestClient();
      restClient.executeMethod(put);
      assertEquals(HttpStatus.SC_OK, put.getStatusCode());
      assertEquals("", put.getResponseBodyAsString().trim());

      // 2. Get with Embedded (given a marshaller, it can unmarshall the result)
      assertEquals(value, cacheFactory.getEmbeddedCache().getAdvancedCache()
            .withEncoding(IdentityEncoder.class, getEncoderClass()).get(key));

      // 3. Get with Memcached (given a marshaller, it can unmarshall the result)
      bytes = (byte[]) cacheFactory.getMemcachedClient().get(key);
      assertEquals(value, marshaller.objectFromByteBuffer(bytes));

      // 4. Get with Hot Rod (given a marshaller, it can unmarshall the result)
      bytes = (byte[]) cacheFactory.getHotRodCache().get(key);
      assertEquals(value, marshaller.objectFromByteBuffer(bytes));
   }
}
