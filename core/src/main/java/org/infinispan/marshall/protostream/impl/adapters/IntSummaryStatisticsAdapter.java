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

   @ProtoField(1)
   long getCount(IntSummaryStatistics statistics) {
      return statistics.getCount();
   }

   @ProtoField(2)
   int getMin(IntSummaryStatistics statistics) {
      return statistics.getMin();
   }

   @ProtoField(3)
   int getMax(IntSummaryStatistics statistics) {
      return statistics.getMax();
   }

   @ProtoField(4)
   long getSum(IntSummaryStatistics statistics) {
      return statistics.getSum();
   }
}
