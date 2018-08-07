package org.infinispan.client.hotrod.query;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;
import static org.testng.Assert.assertNull;
import static org.testng.AssertJUnit.assertEquals;

import java.util.List;

import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.Search;
import org.infinispan.commons.marshall.UTF8StringMarshaller;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.query.dsl.Query;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Test remote queries against Infinispan Directory provider.
 *
 * @author anistor@redhat.com
 * @since 6.0
 */
@Test(testName = "client.hotrod.query.HotRodQueryIspnDirectoryTest", groups = "functional")
public class HotRodQueryIspnDirectoryTest extends HotRodQueryTest {

   @Override
   protected ConfigurationBuilder getConfigurationBuilder() {
      ConfigurationBuilder builder = super.getConfigurationBuilder();
      builder.indexing().addProperty("default.directory_provider", "infinispan");
      return builder;
   }

   public void testReadAsJSON() throws Exception {
      DataFormat acceptJSON = DataFormat.builder().valueType(APPLICATION_JSON).valueMarshaller(new UTF8StringMarshaller()).build();
      RemoteCache<Integer, String> jsonCache = this.remoteCache.withDataFormat(acceptJSON);

      JsonNode user1 = new ObjectMapper().readTree(jsonCache.get(1));

      assertEquals("Tom", user1.get("name").asText());
      assertEquals("Cat", user1.get("surname").asText());

      Query query = Search.getQueryFactory(jsonCache).create("FROM sample_bank_account.User where name = 'Tom'");
      List<String> results = query.list();

      assertEquals(1, query.getResultSize());
      assertNull(query.getProjection());

      JsonNode jsonNode = new ObjectMapper().readTree(results.iterator().next());
      assertEquals("Tom", jsonNode.get("name").asText());
      assertEquals("Cat", jsonNode.get("surname").asText());
   }
}
