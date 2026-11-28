package org.infinispan.query.objectfilter.impl.aggregation;

import static org.assertj.core.api.Assertions.assertThat;

import org.infinispan.query.objectfilter.impl.ql.AggregationFunction;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "query.objectfilter.impl.aggregation.AvgAccumulatorTest")
public class AvgAccumulatorTest {
   @DataProvider
   public Object[][] numericTypes() {
      return new Object[][] {{Integer.class, 1, 2}, {Long.class, 1L, 2L}};
   }

   @Test(dataProvider = "numericTypes")
   public void testFractionalAverage(Class<?> type, Number first, Number second) {
      assertThat(FieldAccumulator.getOutputType(AggregationFunction.AVG, type)).isEqualTo(Double.class);
      AvgAccumulator accumulator = new AvgAccumulator(0, 0, type);
      Object[] row = new Object[1];
      accumulator.init(row);
      accumulator.update(row, first);
      accumulator.update(row, null);
      accumulator.update(row, second);
      accumulator.finish(row);
      assertThat(row[0]).isInstanceOf(Double.class).isEqualTo(1.5d);
   }

   @Test(dataProvider = "numericTypes")
   public void testEmptyAverage(Class<?> type, Number first, Number second) {
      AvgAccumulator accumulator = new AvgAccumulator(0, 0, type);
      Object[] row = new Object[1];
      accumulator.init(row);
      accumulator.update(row, null);
      accumulator.finish(row);
      assertThat(row[0]).isNull();
   }
}
