package org.infinispan.query.impl;

import java.io.IOException;

import org.hibernate.search.query.engine.spi.DocumentExtractor;
import org.hibernate.search.query.engine.spi.EntityInfo;
import org.infinispan.commons.CacheException;

import net.jcip.annotations.NotThreadSafe;

/**
 * Implementation for {@link org.infinispan.query.ResultIterator}. This loads the results only when required
 * and hence differs from {@link EagerIterator} which is the other implementation of ResultIterator.
 *
 * @author Navin Surtani
 * @author Marko Luksa
 * @author Ales Justin
 */
@NotThreadSafe
final class LazyIterator<E> extends AbstractIterator<E> {

   private final DocumentExtractor extractor;

   LazyIterator(DocumentExtractor extractor, QueryResultLoader resultLoader, int fetchSize) {
      super(resultLoader, extractor.getFirstIndex(), extractor.getMaxIndex(), fetchSize);
      this.extractor = extractor;
   }

   @Override
   public void close() {
      extractor.close();
   }

   @Override
   protected EntityInfo loadEntityInfo(int index) {
      try {
         return extractor.extract(index);
      } catch (IOException e) {
         throw new CacheException("Cannot load result at index " + index, e);
      }
   }
}
