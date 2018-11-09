package org.infinispan.query.impl.externalizers;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.query.logging.Log;
import org.infinispan.util.logging.LogFactory;


public class LuceneBooleanQueryExternalizer extends AbstractExternalizer<BooleanQuery> {

   private static final Log log = LogFactory.getLog(LuceneBooleanQueryExternalizer.class, Log.class);

   @Override
   public Set<Class<? extends BooleanQuery>> getTypeClasses() {
      return Collections.singleton(BooleanQuery.class);
   }

   @Override
   public BooleanQuery readObject(final ObjectInput input) throws IOException, ClassNotFoundException {
      final boolean disableCoord = input.readBoolean();
      final float boost = input.readFloat();
      final int minimumNumberShouldMatch = UnsignedNumeric.readUnsignedInt(input);
      final int numberOfClauses = UnsignedNumeric.readUnsignedInt(input);

      BooleanQuery.Builder unserialized = new BooleanQuery.Builder()
              .setDisableCoord(disableCoord)
              .setMinimumNumberShouldMatch(minimumNumberShouldMatch);
      assureNumberOfClausesLimit(numberOfClauses);
      for (int i = 0; i < numberOfClauses; i++) {
         appendReadClause(input, unserialized);
      }
      BooleanQuery booleanQuery = unserialized.build();
      booleanQuery.setBoost(boost);
      return booleanQuery;
   }

   private void appendReadClause(ObjectInput input, BooleanQuery.Builder builder) throws IOException, ClassNotFoundException {
      final Occur occur = (Occur) input.readObject();
      Query q = (Query) input.readObject();
      BooleanClause clause = new BooleanClause(q, occur);
      builder.add(clause);
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
      for (BooleanClause booleanClause : booleanClauses) {
         writeClause(output, booleanClause);
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
      if (numberOfClauses > BooleanQuery.getMaxClauseCount()) {
         log.overridingBooleanQueryMaxClauseCount(numberOfClauses);
         BooleanQuery.setMaxClauseCount(numberOfClauses);
      }
   }
}
