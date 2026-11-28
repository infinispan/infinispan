package org.infinispan.query.aggregation;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.infinispan.commons.api.query.Query;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.sampledomain.IndexedPlayer;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "query.aggregation.QueryAverageValidationTest")
public class QueryAverageValidationTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      return TestCacheManagerFactory.createCacheManager(IndexedPlayer.PlayerSchema.INSTANCE, builder);
   }

   @Test
   public void testAvgStringFieldWithPopulatedData() {
      cache.put("1", new IndexedPlayer("player 1", "PINK", 50));
      assertThatThrownBy(() -> {
         Query<Object[]> query = cache.query("select avg(name) from " + IndexedPlayer.class.getName());
         query.list();
      }).isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("ISPN028513");
   }

   @Test
   public void testAvgStringFieldWithEmptyData() {
      assertThatThrownBy(() -> {
         Query<Object[]> query = cache.query("select avg(name) from " + IndexedPlayer.class.getName());
         query.list();
      }).isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("ISPN028513");
   }

   @Test
   public void testGroupedAvgStringFieldWithPopulatedData() {
      cache.put("1", new IndexedPlayer("player 1", "PINK", 50));
      assertThatThrownBy(() -> {
         Query<Object[]> query = cache.query("select color, avg(name) from " + IndexedPlayer.class.getName() + " group by color");
         query.list();
      }).isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("ISPN028513");
   }

   @Test
   public void testGroupedAvgStringFieldWithEmptyData() {
      assertThatThrownBy(() -> {
         Query<Object[]> query = cache.query("select color, avg(name) from " + IndexedPlayer.class.getName() + " group by color");
         query.list();
      }).isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("ISPN028513");
   }
}
