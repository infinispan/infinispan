package org.infinispan.marshall.exts;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.IntSummaryStatistics;
import java.util.Set;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.NotSerializableException;
import org.infinispan.commons.marshall.UserObjectInput;
import org.infinispan.commons.marshall.UserObjectOutput;
import org.infinispan.commons.util.Util;
import org.infinispan.marshall.core.Ids;

/**
 * Externalizer used for {@link IntSummaryStatistics}.  Note this assumes given fields have specific names to use
 * through reflection.
 *
 * @author wburns
 * @since 8.2
 */
public class IntSummaryStatisticsExternalizer extends AbstractExternalizer<IntSummaryStatistics> {
   static final Field countField;
   static final Field sumField;
   static final Field minField;
   static final Field maxField;

   static final boolean canSerialize;

   static {
      countField = SecurityActions.getField(IntSummaryStatistics.class, "count");
      sumField = SecurityActions.getField(IntSummaryStatistics.class, "sum");
      minField = SecurityActions.getField(IntSummaryStatistics.class, "min");
      maxField = SecurityActions.getField(IntSummaryStatistics.class, "max");

      // We can only properly serialize if all of the fields are non null
      canSerialize = countField != null && sumField != null && minField != null && maxField != null;
   }

   @Override
   public Set<Class<? extends IntSummaryStatistics>> getTypeClasses() {
      return Util.<Class<? extends IntSummaryStatistics>>asSet(IntSummaryStatistics.class);
   }

   private void verifySerialization() {
      if (!canSerialize) {
         throw new NotSerializableException("IntSummaryStatistics is not serializable, fields not available!");
      }
   }

   @Override
   public void writeObject(UserObjectOutput output, IntSummaryStatistics object) throws IOException {
      verifySerialization();
      try {
         output.writeLong(countField.getLong(object));
         output.writeLong(sumField.getLong(object));
         output.writeInt(minField.getInt(object));
         output.writeInt(maxField.getInt(object));
      } catch (IllegalAccessException e) {
         // This can't happen as we force accessibility in the getField
         throw new IOException(e);
      }
   }

   @Override
   public IntSummaryStatistics readObject(UserObjectInput input) throws IOException, ClassNotFoundException {
      verifySerialization();
      IntSummaryStatistics summaryStatistics = new IntSummaryStatistics();
      try {
         countField.setLong(summaryStatistics, input.readLong());
         sumField.setLong(summaryStatistics, input.readLong());
         minField.setInt(summaryStatistics, input.readInt());
         maxField.setInt(summaryStatistics, input.readInt());
      } catch (IllegalAccessException e) {
         // This can't happen as we force accessibility in the getField
         throw new IOException(e);
      }
      return summaryStatistics;
   }

   @Override
   public Integer getId() {
      return Ids.INT_SUMMARY_STATISTICS;
   }
}
