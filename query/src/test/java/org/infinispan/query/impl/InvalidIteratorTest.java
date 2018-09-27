package org.infinispan.query.impl;

import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.hibernate.search.query.engine.spi.DocumentExtractor;
import org.hibernate.search.query.engine.spi.EntityInfo;
import org.infinispan.AdvancedCache;
import org.infinispan.query.backend.KeyTransformationHandler;
import org.mockito.stubbing.Answer;
import org.testng.annotations.Test;

/**
 * Simple test for checking the Iterator Initialization with wrong fetch size.
 *
 * @author Anna Manukyan
 */
@Test(groups = "functional", testName = "query.impl.InvalidIteratorTest")
public class InvalidIteratorTest {

   private List<EntityInfo> entityInfos = new ArrayList<>();

   private AdvancedCache<String, String> cache;

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void testLazyIteratorInitWithInvalidFetchSize() throws IOException {
      DocumentExtractor extractor = mock(DocumentExtractor.class);
      when(extractor.extract(anyInt())).thenAnswer((Answer<EntityInfo>) invocation -> {
         int index = (Integer) invocation.getArguments()[0];
         return entityInfos.get(index);
      });

      cache = mock(AdvancedCache.class);
      new LazyIterator<>(extractor, new EntityLoader(cache, new KeyTransformationHandler(null)), getFetchSize());
   }

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void testEagerIteratorInitWithInvalidFetchSize() {
      cache = mock(AdvancedCache.class);
      new EagerIterator<>(entityInfos, new EntityLoader(cache, new KeyTransformationHandler(null)), getFetchSize());
   }

   private int getFetchSize() {
      return 0;
   }
}
