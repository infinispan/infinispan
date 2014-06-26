package org.infinispan.query.dsl.embedded;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

/**
 * Base for the DSL query tests.
 *
 * @author rvansa@redhat.com
 * @author anistor@redhat.com
 */
public abstract class AbstractQueryDslTest extends SingleCacheManagerTest {

   protected final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");

   protected AbstractQueryDslTest() {
      DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("GMT"));
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cfg = getDefaultStandaloneCacheConfig(true);
      cfg.transaction()
            .transactionMode(TransactionMode.TRANSACTIONAL)
            .indexing().index(Index.ALL)
            .addProperty("default.directory_provider", "ram")
            .addProperty("lucene_version", "LUCENE_CURRENT");
      return TestCacheManagerFactory.createCacheManager(cfg);
   }
}
