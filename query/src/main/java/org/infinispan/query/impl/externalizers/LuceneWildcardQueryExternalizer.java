package org.infinispan.query.impl.externalizers;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.WildcardQuery;
import org.infinispan.commons.marshall.AdvancedExternalizer;

/**
 * @since 9.1
 */
public class LuceneWildcardQueryExternalizer implements AdvancedExternalizer<WildcardQuery> {

   @Override
   public Set<Class<? extends WildcardQuery>> getTypeClasses() {
      return Collections.singleton(WildcardQuery.class);
   }

   @Override
   public Integer getId() {
      return ExternalizerIds.LUCENE_QUERY_WILDCARD;
   }

   @Override
   public void writeObject(ObjectOutput output, WildcardQuery wildcardQuery) throws IOException {
      output.writeObject(wildcardQuery.getTerm());
   }

   @Override
   public WildcardQuery readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      return new WildcardQuery((Term) input.readObject());
   }
}
