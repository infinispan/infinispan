package org.infinispan.query.impl.externalizers;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.infinispan.commons.marshall.AbstractExternalizer;

/**
 * @author gustavonalle
 * @since 7.1
 */
public class LuceneMatchAllQueryExternalizer extends AbstractExternalizer<MatchAllDocsQuery> {

   @Override
   public Integer getId() {
      return ExternalizerIds.LUCENE_QUERY_MATCH_ALL;
   }

   @Override
   public Set<Class<? extends MatchAllDocsQuery>> getTypeClasses() {
      return Collections.singleton(MatchAllDocsQuery.class);
   }

   @Override
   public void writeObject(ObjectOutput output, MatchAllDocsQuery object) throws IOException {
   }

   @Override
   public MatchAllDocsQuery readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      return new MatchAllDocsQuery();
   }
}
