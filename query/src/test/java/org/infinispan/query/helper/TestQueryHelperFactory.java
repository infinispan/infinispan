package org.infinispan.query.helper;

import org.infinispan.Cache;
import org.infinispan.query.backend.QueryHelper;

import java.util.Properties;

/**
 * Creates a test query helper
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class TestQueryHelperFactory {
   public static QueryHelper createTestQueryHelperInstance(Cache<?, ?> cache, Class... classes) {
      Properties p = new Properties();
      p.setProperty("hibernate.search.default.directory_provider", "org.hibernate.search.store.RAMDirectoryProvider");
      return new QueryHelper(cache, p, classes);
   }
}
