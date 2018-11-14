package org.infinispan.marshall.exts;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.IntSummaryStatistics;
import java.util.Set;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.NotSerializableException;
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
   static final Constructor<IntSummaryStatistics> constructor;

   static {
      constructor = SecurityActions.getConstructor(
              IntSummaryStatistics.class, long.class, int.class, int.class, long.class);

      if (constructor != null) {
         // since JDK 10, *SummaryStatistics have parametrized constructors, so reflection can be avoided

         countField = null;
         sumField = null;
         minField = null;
         maxField = null;

         canSerialize = true;
      } else {
         countField = SecurityActions.getField(IntSummaryStatistics.class, "count");
         sumField = SecurityActions.getField(IntSummaryStatistics.class, "sum");
         minField = SecurityActions.getField(IntSummaryStatistics.class, "min");
         maxField = SecurityActions.getField(IntSummaryStatistics.class, "max");

         // We can only properly serialize if all of the fields are non null
         canSerialize = countField != null && sumField != null && minField != null && maxField != null;
      }
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
   public void writeObject(ObjectOutput output, IntSummaryStatistics object) throws IOException {
      verifySerialization();
      output.writeLong(object.getCount());
      output.writeLong(object.getSum());
      output.writeInt(object.getMin());
      output.writeInt(object.getMax());
   }

   @Override
   public IntSummaryStatistics readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      verifySerialization();
      final IntSummaryStatistics summaryStatistics;

      final long count = input.readLong();
      final long sum = input.readLong();
      final int min = input.readInt();
      final int max = input.readInt();

      if (constructor != null) {
         // JDK 10+, pass values to constructor
         try {
            summaryStatistics = constructor.newInstance(count, min, max, sum);
         } catch (ReflectiveOperationException e) {
            throw new IOException(e);
         }
      } else {
         // JDK 9 or older, fall back to reflection
         try {
            summaryStatistics = new IntSummaryStatistics();
            countField.setLong(summaryStatistics, count);
            sumField.setLong(summaryStatistics, sum);
            minField.setInt(summaryStatistics, min);
            maxField.setInt(summaryStatistics, max);
         } catch (IllegalAccessException e) {
            // This can't happen as we force accessibility in the getField
            throw new IOException(e);
         }
      }
      return summaryStatistics;
   }

   @Override
   public Integer getId() {
      return Ids.INT_SUMMARY_STATISTICS;
   }
}
