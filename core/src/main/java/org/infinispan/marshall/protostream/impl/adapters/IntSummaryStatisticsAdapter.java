package org.infinispan.marshall.protostream.impl.adapters;

import java.util.IntSummaryStatistics;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoAdapter;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

@ProtoAdapter(IntSummaryStatistics.class)
@ProtoTypeId(ProtoStreamTypeIds.ADAPTER_INT_SUMMARY_STATISTICS)
public class IntSummaryStatisticsAdapter {

   @ProtoFactory
   static IntSummaryStatistics protoFactory(long count, int min, int max, long sum) {
      return new IntSummaryStatistics(count, min, max, sum);
   }

   @ProtoField(number = 1, defaultValue = "0")
   long getCount(IntSummaryStatistics statistics) {
      return statistics.getCount();
   }

   @ProtoField(number = 2, defaultValue = "0")
   int getMin(IntSummaryStatistics statistics) {
      return statistics.getMin();
   }

   @ProtoField(number = 3, defaultValue = "0")
   int getMax(IntSummaryStatistics statistics) {
      return statistics.getMax();
   }

   @ProtoField(number = 4, defaultValue = "0")
   long getSum(IntSummaryStatistics statistics) {
      return statistics.getSum();
   }
}
