package org.infinispan.query.impl.externalizers;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.Util;

/**
 * WARNING: this Externalizer implementation drops some state associated to the TermQuery instance.
 *
 * A TermQuery is potentially connected to many open resources whose reference will
 * be severed by this externalizer, and also supports advanced options to set additional
 * state which will not be taken into consideration at this stage.
 */
public class LuceneTermQueryExternalizer extends AbstractExternalizer<TermQuery> {

   @Override
   public Set<Class<? extends TermQuery>> getTypeClasses() {
      return Util.<Class<? extends TermQuery>>asSet(TermQuery.class);
   }

   @Override
   public TermQuery readObject(final ObjectInput input) throws IOException, ClassNotFoundException {
      final float boost = input.readFloat();
      final Term term = (Term) input.readObject();
      TermQuery termQuery = new TermQuery(term);
      termQuery.setBoost(boost);
      return termQuery;
   }

   @Override
   public void writeObject(final ObjectOutput output, final TermQuery query) throws IOException {
      output.writeFloat(query.getBoost());
      output.writeObject(query.getTerm());
   }

   @Override
   public Integer getId() {
      return ExternalizerIds.LUCENE_QUERY_TERM;
   }

}
