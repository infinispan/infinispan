package org.infinispan.query.helper;

import static org.assertj.core.api.Assertions.assertThat;

import org.infinispan.Cache;
import org.infinispan.search.mapper.mapping.SearchMapping;

public final class StaticTestingErrorHandler {

   public static void assertAllGood(Cache... caches) {
      for (Cache cache : caches) {
         assertAllGood(cache);
      }
   }

   public static void assertAllGood(Cache cache) {
      SearchMapping searchMapping = TestQueryHelperFactory.extractSearchMapping(cache);

      assertThat(searchMapping.genericIndexingFailures()).isZero();
      assertThat(searchMapping.entityIndexingFailures()).isZero();
   }
}
