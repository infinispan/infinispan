package org.infinispan.query.impl;

import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.List;

import org.hibernate.search.query.engine.spi.DocumentExtractor;
import org.hibernate.search.query.engine.spi.EntityInfo;
import org.infinispan.AdvancedCache;
import org.infinispan.query.backend.KeyTransformationHandler;
import org.testng.annotations.Test;

/**
 * Test whether LazyIterator and EagerIterator can handle a very large fetch size (Integer.MAX_VALUE). This makes sure
 * that neither of the two iterators actually try to create an array of size Integer.MAX_VALUE in such cases.
 *
 * @author Marko Luksa
 */
@Test(groups = "functional", testName = "query.impl.VeryLargeFetchSizeTest")
public class VeryLargeFetchSizeTest {

   private static final int VERY_LARGE_FETCH_SIZE = Integer.MAX_VALUE;

   private List<EntityInfo> entityInfos = new ArrayList<>();

   private AdvancedCache<String, String> cache;

   @Test
   public void testLazyIteratorHandlesVeryLargeFetchSize() {
      cache = mock(AdvancedCache.class);
      DocumentExtractor extractor = mock(DocumentExtractor.class);
      new LazyIterator<>(extractor, new EntityLoader(cache, new KeyTransformationHandler(null)), VERY_LARGE_FETCH_SIZE);
   }

   @Test
   public void testEagerIteratorHandlesVeryLargeFetchSize() {
      cache = mock(AdvancedCache.class);
      new EagerIterator<>(entityInfos, new EntityLoader(cache, new KeyTransformationHandler(null)), VERY_LARGE_FETCH_SIZE);
   }
}
