package org.infinispan.marshall.exts;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.LongSummaryStatistics;
import java.util.Set;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.NotSerializableException;
import org.infinispan.commons.marshall.UserObjectInput;
import org.infinispan.commons.marshall.UserObjectOutput;
import org.infinispan.commons.util.Util;
import org.infinispan.marshall.core.Ids;

/**
 * Externalizer used for {@link LongSummaryStatistics}.  Note this assumes given fields have specific names to use
 * through reflection.
 *
 * @author wburns
 * @since 8.2
 */
public class LongSummaryStatisticsExternalizer extends AbstractExternalizer<LongSummaryStatistics> {
   static final Field countField;
   static final Field sumField;
   static final Field minField;
   static final Field maxField;

   static final boolean canSerialize;

   static {
      countField = SecurityActions.getField(LongSummaryStatistics.class, "count");
      sumField = SecurityActions.getField(LongSummaryStatistics.class, "sum");
      minField = SecurityActions.getField(LongSummaryStatistics.class, "min");
      maxField = SecurityActions.getField(LongSummaryStatistics.class, "max");

      // We can only properly serialize if all of the fields are non null
      canSerialize = countField != null && sumField != null && minField != null && maxField != null;
   }

   @Override
   public Set<Class<? extends LongSummaryStatistics>> getTypeClasses() {
      return Util.<Class<? extends LongSummaryStatistics>>asSet(LongSummaryStatistics.class);
   }

   private void verifySerialization() {
      if (!canSerialize) {
         throw new NotSerializableException("LongSummaryStatistics is not serializable, fields not available!");
      }
   }

   @Override
   public void writeObject(UserObjectOutput output, LongSummaryStatistics object) throws IOException {
      verifySerialization();
      try {
         output.writeLong(countField.getLong(object));
         output.writeLong(sumField.getLong(object));
         output.writeLong(minField.getLong(object));
         output.writeLong(maxField.getLong(object));
      } catch (IllegalAccessException e) {
         // This can't happen as we force accessibility in the getField
         throw new IOException(e);
      }
   }

   @Override
   public LongSummaryStatistics readObject(UserObjectInput input) throws IOException, ClassNotFoundException {
      verifySerialization();
      LongSummaryStatistics summaryStatistics = new LongSummaryStatistics();
      try {
         countField.setLong(summaryStatistics, input.readLong());
         sumField.setLong(summaryStatistics, input.readLong());
         minField.setLong(summaryStatistics, input.readLong());
         maxField.setLong(summaryStatistics, input.readLong());
      } catch (IllegalAccessException e) {
         // This can't happen as we force accessibility in the getField
         throw new IOException(e);
      }
      return summaryStatistics;
   }

   @Override
   public Integer getId() {
      return Ids.LONG_SUMMARY_STATISTICS;
   }
}
