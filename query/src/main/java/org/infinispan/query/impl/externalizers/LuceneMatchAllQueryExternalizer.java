package org.infinispan.query.impl.externalizers;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.Util;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

/**
 *
 * @author gustavonalle
 * @since 7.1
 */
@SuppressWarnings("unchecked")
public class LuceneMatchAllQueryExternalizer extends AbstractExternalizer<MatchAllDocsQuery> {
   @Override
   public Set<Class<? extends MatchAllDocsQuery>> getTypeClasses() {
      return Util.<Class<? extends MatchAllDocsQuery>>asSet(MatchAllDocsQuery.class);
   }

   @Override
   public void writeObject(ObjectOutput output, MatchAllDocsQuery object) throws IOException {
   }

   @Override
   public MatchAllDocsQuery readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      return new MatchAllDocsQuery();
   }
}
