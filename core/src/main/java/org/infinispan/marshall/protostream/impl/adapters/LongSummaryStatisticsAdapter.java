package org.infinispan.marshall.protostream.impl.adapters;

import java.util.LongSummaryStatistics;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoAdapter;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

@ProtoAdapter(LongSummaryStatistics.class)
@ProtoTypeId(ProtoStreamTypeIds.ADAPTER_LONG_SUMMARY_STATISTICS)
public class LongSummaryStatisticsAdapter {

   @ProtoFactory
   static LongSummaryStatistics protoFactory(long count, long min, long max, long sum) {
      return new LongSummaryStatistics(count, min, max, sum);
   }

   @ProtoField(number = 1, defaultValue = "0")
   long getCount(LongSummaryStatistics statistics) {
      return statistics.getCount();
   }

   @ProtoField(number = 2, defaultValue = "0")
   long getMin(LongSummaryStatistics statistics) {
      return statistics.getMin();
   }

   @ProtoField(number = 3, defaultValue = "0")
   long getMax(LongSummaryStatistics statistics) {
      return statistics.getMax();
   }

   @ProtoField(number = 4, defaultValue = "0")
   long getSum(LongSummaryStatistics statistics) {
      return statistics.getSum();
   }
}
