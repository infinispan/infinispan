package org.infinispan.marshall.protostream.impl.adapters;

import java.util.DoubleSummaryStatistics;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoAdapter;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

@ProtoAdapter(DoubleSummaryStatistics.class)
@ProtoTypeId(ProtoStreamTypeIds.ADAPTER_DOUBLE_SUMMARY_STATISTICS)
public class DoubleSummaryStatisticsAdapter {

   @ProtoFactory
   static DoubleSummaryStatistics protoFactory(long count, double min, double max, double sum) {
      return new DoubleSummaryStatistics(count, min, max, sum);
   }

   @ProtoField(1)
   long getCount(DoubleSummaryStatistics statistics) {
      return statistics.getCount();
   }

   @ProtoField(2)
   double getMin(DoubleSummaryStatistics statistics) {
      return statistics.getMin();
   }

   @ProtoField(3)
   double getMax(DoubleSummaryStatistics statistics) {
      return statistics.getMax();
   }

   @ProtoField(4)
   double getSum(DoubleSummaryStatistics statistics) {
      return statistics.getSum();
   }
}
