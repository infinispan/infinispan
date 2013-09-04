package org.infinispan.query.remote.search;

import com.google.protobuf.Descriptors;
import org.apache.lucene.search.Query;
import org.hibernate.hql.lucene.LuceneQueryParsingResult;

import java.util.List;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
public class IspnLuceneQueryParsingResult extends LuceneQueryParsingResult {

   private final Descriptors.Descriptor targetType;

   public IspnLuceneQueryParsingResult(Query query, Descriptors.Descriptor targetType, Class<?> targetEntity, List<String> projections) {
      super(query, targetEntity, projections);
      this.targetType = targetType;
   }

   public Descriptors.Descriptor getTargetType() {
      return targetType;
   }
}
