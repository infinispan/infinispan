package org.infinispan.marshall.exts;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.LongSummaryStatistics;
import java.util.Set;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.NotSerializableException;
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
   static final Constructor<LongSummaryStatistics> constructor;

   static {
      constructor = SecurityActions
              .getConstructor(LongSummaryStatistics.class, long.class, long.class, long.class, long.class);

      if (constructor != null) {
         // since JDK 10, *SummaryStatistics have parametrized constructors, so reflection can be avoided

         countField = null;
         sumField = null;
         minField = null;
         maxField = null;

         canSerialize = true;
      } else {
         countField = SecurityActions.getField(LongSummaryStatistics.class, "count");
         sumField = SecurityActions.getField(LongSummaryStatistics.class, "sum");
         minField = SecurityActions.getField(LongSummaryStatistics.class, "min");
         maxField = SecurityActions.getField(LongSummaryStatistics.class, "max");

         // We can only properly serialize if all of the fields are non null
         canSerialize = countField != null && sumField != null && minField != null && maxField != null;
      }
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
   public void writeObject(ObjectOutput output, LongSummaryStatistics object) throws IOException {
      verifySerialization();
      output.writeLong(object.getCount());
      output.writeLong(object.getSum());
      output.writeLong(object.getMin());
      output.writeLong(object.getMax());
   }

   @Override
   public LongSummaryStatistics readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      verifySerialization();
      final LongSummaryStatistics summaryStatistics;

      final long count = input.readLong();
      final long sum = input.readLong();
      final long min = input.readLong();
      final long max = input.readLong();

      if (constructor != null) {
         // JDK 10+, pass values to constructor
         try {
            summaryStatistics = constructor.newInstance(count, min, max, sum);
         } catch (ReflectiveOperationException e) {
            throw new IOException(e);
         }
      } else {
         // JDK 9 or older, fall back to reflection
         summaryStatistics = new LongSummaryStatistics();
         try {
            countField.setLong(summaryStatistics, count);
            sumField.setLong(summaryStatistics, sum);
            minField.setLong(summaryStatistics, min);
            maxField.setLong(summaryStatistics, max);
         } catch (IllegalAccessException e) {
            // This can't happen as we force accessibility in the getField
            throw new IOException(e);
         }
      }
      return summaryStatistics;
   }

   @Override
   public Integer getId() {
      return Ids.LONG_SUMMARY_STATISTICS;
   }
}
