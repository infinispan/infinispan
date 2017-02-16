package org.infinispan.query.indexmanager;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.hibernate.search.backend.LuceneWork;
import org.infinispan.query.backend.KeyTransformationHandler;

/**
 * Manages conversions of {@link org.hibernate.search.backend.LuceneWork}.
 *
 * @since 9.0
 */
public final class LuceneWorkConverter {

   private LuceneWorkConverter() {
   }

   public static List<LuceneWork> transformKeysToString(Collection<LuceneWork> works,
                                                        KeyTransformationHandler handler) {
      List<LuceneWork> transformedWorks = new ArrayList<>(works.size());
      transformedWorks.addAll(works.stream().map(work -> transformKeysToString(work, handler))
            .collect(Collectors.toList()));
      return transformedWorks;
   }

   static LuceneWork transformKeysToString(LuceneWork luceneWork, KeyTransformationHandler handler) {
      return luceneWork.acceptIndexWorkVisitor(LuceneWorkTransformationVisitor.INSTANCE, handler);
   }

}
