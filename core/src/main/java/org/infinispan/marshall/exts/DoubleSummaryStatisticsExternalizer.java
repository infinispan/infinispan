package org.infinispan.marshall.exts;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.DoubleSummaryStatistics;
import java.util.Set;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.NotSerializableException;
import org.infinispan.commons.util.Util;
import org.infinispan.marshall.core.Ids;

/**
 * Externalizer used for {@link DoubleSummaryStatistics}.  Note this assumes given fields have specific names to use
 * through reflection.
 *
 * @author wburns
 * @since 8.2
 */
public class DoubleSummaryStatisticsExternalizer extends AbstractExternalizer<DoubleSummaryStatistics> {

   private final static String CONSTRUCTOR_CALL_ERROR_MSG =
           "Unable to create instance of %s via [%s] with parameters (%s, %s, %s, %s)";

   static final Field countField;
   static final Field sumField;
   static final Field minField;
   static final Field maxField;

   static final boolean canSerialize;
   static final Constructor<DoubleSummaryStatistics> constructor;

   static {
      constructor = SecurityActions.getConstructor(
              DoubleSummaryStatistics.class, long.class, double.class, double.class, double.class);

      if (constructor != null) {
         // since JDK 10, *SummaryStatistics have parametrized constructors, so reflection can be avoided

         countField = null;
         sumField = null;
         minField = null;
         maxField = null;

         canSerialize = true;
      } else {
         countField = SecurityActions.getField(DoubleSummaryStatistics.class, "count");
         sumField = SecurityActions.getField(DoubleSummaryStatistics.class, "sum");
         minField = SecurityActions.getField(DoubleSummaryStatistics.class, "min");
         maxField = SecurityActions.getField(DoubleSummaryStatistics.class, "max");

         // We can only properly serialize if all of the fields are non null
         canSerialize = countField != null && sumField != null && minField != null && maxField != null;
      }
   }

   @Override
   public Set<Class<? extends DoubleSummaryStatistics>> getTypeClasses() {
      return Util.<Class<? extends DoubleSummaryStatistics>>asSet(DoubleSummaryStatistics.class);
   }

   private void verifySerialization() {
      if (!canSerialize) {
         throw new NotSerializableException("DoubleSummaryStatistics is not serializable, fields not available!");
      }
   }

   @Override
   public void writeObject(ObjectOutput output, DoubleSummaryStatistics object) throws IOException {
      verifySerialization();
      output.writeLong(object.getCount());
      output.writeDouble(object.getSum());
      // To stay backward compatible with older versions of infinispan, write 0s for the compensation and simpleSum
      // fields. The API doesn't allow for obtaining nor setting values of these _implementation specific_ fields.
      // This means precision can be lost.
      output.writeDouble(0); // sumCompensation
      output.writeDouble(0); // simpleSum
      output.writeDouble(object.getMin());
      output.writeDouble(object.getMax());
   }

   @Override
   public DoubleSummaryStatistics readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      verifySerialization();
      final DoubleSummaryStatistics summaryStatistics;

      final long count = input.readLong();
      final double sum = input.readDouble();
      final double sumCompensation = input.readDouble(); // ignored, see writeObject()
      final double simpleSum = input.readDouble(); // ignored, see writeObject()
      final double min = input.readDouble();
      final double max = input.readDouble();

      if (constructor != null) {
         // JDK 10+, pass values to constructor
         try {
            summaryStatistics = constructor.newInstance(count, min, max, sum);
         } catch (ReflectiveOperationException e) {
            throw new IOException(String.format(CONSTRUCTOR_CALL_ERROR_MSG,
                    DoubleSummaryStatistics.class, constructor.toString(), count, min, max, sum), e);
         }
      } else {
         // JDK 9 or older, fall back to reflection
         summaryStatistics = new DoubleSummaryStatistics();
         try {
            countField.setLong(summaryStatistics, count);
            sumField.setDouble(summaryStatistics, sum);
            minField.setDouble(summaryStatistics, min);
            maxField.setDouble(summaryStatistics, max);
         } catch (IllegalAccessException e) {
            // This can't happen as we force accessibility in the getField
            throw new IOException(e);
         }
      }
      return summaryStatistics;
   }

   @Override
   public Integer getId() {
      return Ids.DOUBLE_SUMMARY_STATISTICS;
   }
}
