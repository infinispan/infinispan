package org.infinispan.query.impl.externalizers;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import java.util.Set;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.Util;


public class LuceneBooleanQueryExternalizer extends AbstractExternalizer<BooleanQuery> {

   @Override
   public Set<Class<? extends BooleanQuery>> getTypeClasses() {
      return Util.<Class<? extends BooleanQuery>>asSet(BooleanQuery.class);
   }

   @Override
   public BooleanQuery readObject(final ObjectInput input) throws IOException, ClassNotFoundException {
      final boolean disableCoord = input.readBoolean();
      final BooleanQuery unserialized = new BooleanQuery(disableCoord);
      unserialized.setBoost(input.readFloat());
      unserialized.setMinimumNumberShouldMatch(UnsignedNumeric.readUnsignedInt(input));
      final int numberOfClauses = UnsignedNumeric.readUnsignedInt(input);
      assureNumberOfClausesLimit(numberOfClauses);
      final BooleanClause[] booleanClauses = new BooleanClause[numberOfClauses];
      //We take advantage of the following method not making a defensive copy:
      final List<BooleanClause> clauses = unserialized.clauses();
      for (int i=0; i<numberOfClauses; i++) {
         appendReadClause(input, clauses);
      }
      return unserialized;
   }

   private void appendReadClause(ObjectInput input, List<BooleanClause> clauses) throws IOException, ClassNotFoundException {
      final Occur occur = (Occur) input.readObject();
      Query q = (Query) input.readObject();
      BooleanClause clause = new BooleanClause(q, occur);
      clauses.add(clause);
   }

   private void writeClause(final ObjectOutput output, final BooleanClause booleanClause) throws IOException {
      output.writeObject(booleanClause.getOccur());
      output.writeObject(booleanClause.getQuery());
   }

   @Override
   public void writeObject(final ObjectOutput output, final BooleanQuery query) throws IOException {
      output.writeBoolean(query.isCoordDisabled());
      output.writeFloat(query.getBoost());
      UnsignedNumeric.writeUnsignedInt(output, query.getMinimumNumberShouldMatch());
      final List<BooleanClause> booleanClauses = query.clauses();
      final int numberOfClauses = booleanClauses.size();
      UnsignedNumeric.writeUnsignedInt(output, numberOfClauses);
      for (int i=0; i<numberOfClauses; i++) {
         writeClause(output, booleanClauses.get(i));
      }
   }

   @Override
   public Integer getId() {
      return ExternalizerIds.LUCENE_QUERY_BOOLEAN;
   }

   /**
    * BooleanQuery has a static (but reconfigurable) limit for the number of clauses.
    * If any node was able to bypass this limit, we'll need to assume that this limit
    * was somehow relaxed and some point in time, so we need to apply the same configuration
    * to this node.
    *
    * @param numberOfClauses The number of clauses being deserialized
    */
   private static void assureNumberOfClausesLimit(int numberOfClauses) {
      final int maxClauseLimit = BooleanQuery.getMaxClauseCount();
      if (numberOfClauses>maxClauseLimit) {
         BooleanQuery.setMaxClauseCount(numberOfClauses);
      }
   }

}
