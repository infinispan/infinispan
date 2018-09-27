package org.infinispan.query.impl;

import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hibernate.search.query.engine.spi.DocumentExtractor;
import org.hibernate.search.query.engine.spi.EntityInfo;
import org.infinispan.query.backend.KeyTransformationHandler;
import org.mockito.stubbing.Answer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * @author Navin Surtani
 */
@Test(groups = "functional", testName = "query.impl.LazyIteratorTest")
public class LazyIteratorTest extends EagerIteratorTest {
   private DocumentExtractor extractor;

   @BeforeMethod
   public void setUp() throws Exception {
      super.setUp();

      extractor = mock(DocumentExtractor.class);
      when(extractor.getMaxIndex()).thenReturn(entityInfos.size() - 1);
      when(extractor.extract(anyInt())).thenAnswer((Answer<EntityInfo>) invocation -> {
         int index = (Integer) invocation.getArguments()[0];
         return entityInfos.get(index);
      });

      iterator = new LazyIterator<>(extractor, new EntityLoader(cache, new KeyTransformationHandler(null)), getFetchSize());
   }

   @AfterMethod(alwaysRun = false)
   public void tearDown() {
      iterator.close();
      verify(extractor).close();
      super.tearDown();
   }
}
