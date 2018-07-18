package org.infinispan.marshall.exts;

import java.io.IOException;
import java.io.ObjectInput;
import java.lang.reflect.Field;
import java.util.DoubleSummaryStatistics;
import java.util.Set;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.NotSerializableException;
import org.infinispan.commons.marshall.UserObjectOutput;
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
   static final Field countField;
   static final Field sumField;
   static final Field sumCompensationField;
   static final Field simpleSumField;
   static final Field minField;
   static final Field maxField;

   static final boolean canSerialize;

   static {
      countField = SecurityActions.getField(DoubleSummaryStatistics.class, "count");
      sumField = SecurityActions.getField(DoubleSummaryStatistics.class, "sum");
      sumCompensationField = SecurityActions.getField(DoubleSummaryStatistics.class, "sumCompensation");
      simpleSumField = SecurityActions.getField(DoubleSummaryStatistics.class, "simpleSum");
      minField = SecurityActions.getField(DoubleSummaryStatistics.class, "min");
      maxField = SecurityActions.getField(DoubleSummaryStatistics.class, "max");

      // We can only properly serialize if all of the fields are non null
      canSerialize = countField != null && sumField != null && sumCompensationField != null && simpleSumField != null
              && minField != null && maxField != null;
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
   public void writeObject(UserObjectOutput output, DoubleSummaryStatistics object) throws IOException {
      verifySerialization();
      try {
         output.writeLong(countField.getLong(object));
         output.writeDouble(sumField.getDouble(object));
         output.writeDouble(sumCompensationField.getDouble(object));
         output.writeDouble(simpleSumField.getDouble(object));
         output.writeDouble(minField.getDouble(object));
         output.writeDouble(maxField.getDouble(object));
      } catch (IllegalAccessException e) {
         // This can't happen as we force accessibility in the getField
         throw new IOException(e);
      }
   }

   @Override
   public DoubleSummaryStatistics readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      verifySerialization();
      DoubleSummaryStatistics summaryStatistics = new DoubleSummaryStatistics();
      try {
         countField.setLong(summaryStatistics, input.readLong());
         sumField.setDouble(summaryStatistics, input.readDouble());
         sumCompensationField.setDouble(summaryStatistics, input.readDouble());
         simpleSumField.setDouble(summaryStatistics, input.readDouble());
         minField.setDouble(summaryStatistics, input.readDouble());
         maxField.setDouble(summaryStatistics, input.readDouble());
      } catch (IllegalAccessException e) {
         // This can't happen as we force accessibility in the getField
         throw new IOException(e);
      }
      return summaryStatistics;
   }

   @Override
   public Integer getId() {
      return Ids.DOUBLE_SUMMARY_STATISTICS;
   }
}
