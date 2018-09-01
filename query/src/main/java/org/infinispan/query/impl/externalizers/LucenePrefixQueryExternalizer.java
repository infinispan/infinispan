package org.infinispan.query.impl.externalizers;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.PrefixQuery;
import org.infinispan.commons.marshall.AbstractExternalizer;

/**
 * @since 9.1
 */
public class LucenePrefixQueryExternalizer extends AbstractExternalizer<PrefixQuery> {

   @Override
   public Set<Class<? extends PrefixQuery>> getTypeClasses() {
      return Collections.singleton(PrefixQuery.class);
   }

   @Override
   public void writeObject(ObjectOutput output, PrefixQuery prefixQuery) throws IOException {
      output.writeObject(prefixQuery.getPrefix());
   }

   @Override
   public PrefixQuery readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      return new PrefixQuery((Term) input.readObject());
   }

   @Override
   public Integer getId() {
      return ExternalizerIds.LUCENE_QUERY_PREFIX;
   }
}
