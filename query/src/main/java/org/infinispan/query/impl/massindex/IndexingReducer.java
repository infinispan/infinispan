package org.infinispan.query.impl.massindex;

import java.util.Iterator;

import org.hibernate.search.backend.LuceneWork;
import org.infinispan.distexec.mapreduce.Reducer;

public final class IndexingReducer implements Reducer<Object, LuceneWork> {

   @Override
   public LuceneWork reduce(Object reducedKey, Iterator<LuceneWork> iter) {
      return null;
   }

}
