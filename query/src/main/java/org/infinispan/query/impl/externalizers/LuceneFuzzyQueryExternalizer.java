package org.infinispan.query.impl.externalizers;

import static org.infinispan.commons.util.ReflectionUtil.getValue;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.FuzzyQuery;
import org.infinispan.commons.marshall.AbstractExternalizer;

public class LuceneFuzzyQueryExternalizer extends AbstractExternalizer<FuzzyQuery> {

   private static final String MAX_EXPANSIONS_FIELD = "maxExpansions";

   @Override
   public Set<Class<? extends FuzzyQuery>> getTypeClasses() {
      return Collections.singleton(FuzzyQuery.class);
   }

   @Override
   public void writeObject(ObjectOutput output, FuzzyQuery object) throws IOException {
      output.writeObject(object.getTerm());
      output.writeInt(object.getPrefixLength());
      output.writeBoolean(object.getTranspositions());
      output.writeInt(object.getMaxEdits());
      output.writeInt((int) getValue(object, MAX_EXPANSIONS_FIELD));
   }

   @Override
   public FuzzyQuery readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      Term term = (Term) input.readObject();
      int prefixLength = input.readInt();
      boolean transpositions = input.readBoolean();
      int maxEdits = input.readInt();
      int maxExpansions = input.readInt();
      return new FuzzyQuery(term, maxEdits, prefixLength, maxExpansions, transpositions);
   }

   @Override
   public Integer getId() {
      return ExternalizerIds.LUCENE_QUERY_FUZZY;
   }
}
