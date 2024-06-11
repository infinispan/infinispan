package org.infinispan.client.hotrod.query;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;

import java.util.List;

import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.commons.api.query.Query;
import org.infinispan.commons.api.query.QueryResult;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.marshall.UTF8StringMarshaller;
import org.testng.annotations.Test;


/**
 * Test remote queries against Infinispan Directory provider.
 *
 * @author anistor@redhat.com
 * @since 6.0
 */
@Test(testName = "client.hotrod.query.HotRodQueryIspnDirectoryTest", groups = "functional")
public class HotRodQueryIspnDirectoryTest extends HotRodQueryTest {

   public void testReadAsJSON() {
      DataFormat acceptJSON = DataFormat.builder().valueType(APPLICATION_JSON).valueMarshaller(new UTF8StringMarshaller()).build();
      RemoteCache<Integer, String> jsonCache = remoteCache.withDataFormat(acceptJSON);

      Json user1 = Json.read(jsonCache.get(1));

      assertEquals("Tom", user1.at("name").asString());
      assertEquals("Cat", user1.at("surname").asString());

      Query<String> query = jsonCache.query("FROM sample_bank_account.User WHERE name = :name");
      query.maxResults(10).startOffset(0).setParameter("name", "Tom");

      QueryResult<String> result = query.execute();
      List<String> results = result.list();

      assertEquals(1, query.execute().count().value());
      assertFalse(query.hasProjections());

      Json jsonNode = Json.read(results.iterator().next());
      assertEquals("Tom", jsonNode.at("name").asString());
      assertEquals("Cat", jsonNode.at("surname").asString());

      results = query.execute().list();
      assertEquals(1, results.size());
   }
}
