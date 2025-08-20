package org.infinispan.query.core.stats.impl;

import static org.infinispan.query.core.stats.impl.LocalQueryStatisticsTest.QueryType.HYBRID;
import static org.infinispan.query.core.stats.impl.LocalQueryStatisticsTest.QueryType.INDEX_DISTRIBUTED;
import static org.infinispan.query.core.stats.impl.LocalQueryStatisticsTest.QueryType.INDEX_LOCAL;
import static org.infinispan.query.core.stats.impl.LocalQueryStatisticsTest.QueryType.NON_INDEXED;
import static org.testng.AssertJUnit.assertEquals;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import org.testng.annotations.Test;

@Test(groups = "unit", testName = "query.core.impl.LocalQueryStatisticsTest")
public class LocalQueryStatisticsTest {

   public static final int THREADS = 10;
   public static final int MAX_SAMPLE_LATENCY = 10_000;
   public static final int SAMPLE_SIZE = 1_000_000;
   private static final Random RANDOM = new Random(0);

   enum QueryType {INDEX_LOCAL, INDEX_DISTRIBUTED, HYBRID, NON_INDEXED}

   private final Map<Query, Long> timePerQuery = LongStream.rangeClosed(1, SAMPLE_SIZE).boxed()
         .collect(Collectors.toMap(Query::random, l -> (long) RANDOM.nextInt(MAX_SAMPLE_LATENCY)));

   @Test
   public void testRecord() throws Exception {
      LocalQueryStatistics statistics = new LocalQueryStatistics();
      ExecutorService executorService = Executors.newFixedThreadPool(THREADS);

      BlockingQueue<Entry<Query, Long>> data = new LinkedBlockingDeque<>(timePerQuery.entrySet());

      CountDownLatch countDownLatch = new CountDownLatch(1);
      for (int i = 1; i <= THREADS; i++) {
         executorService.submit(() -> {
            try {
               countDownLatch.await();
               while (!data.isEmpty()) {
                  Entry<Query, Long> take = data.poll(1, TimeUnit.SECONDS);
                  if (take == null) continue;
                  Query q = take.getKey();
                  Long time = take.getValue();
                  switch (q.getType()) {
                     case INDEX_LOCAL:
                        statistics.localIndexedQueryExecuted(q.str, time);
                        break;
                     case HYBRID:
                        statistics.hybridQueryExecuted(q.str, time);
                        break;
                     case INDEX_DISTRIBUTED:
                        statistics.distributedIndexedQueryExecuted(q.str, time);
                        break;
                     case NON_INDEXED:
                        statistics.nonIndexedQueryExecuted(q.str, time);
                        break;
                  }
                  statistics.entityLoaded(time / 2);
               }
            } catch (InterruptedException ignored) {
            }
         });
      }
      countDownLatch.countDown();
      executorService.shutdown();
      executorService.awaitTermination(30, TimeUnit.SECONDS);

      assertEquals(count(INDEX_LOCAL), statistics.getLocalIndexedQueryCount());
      assertEquals(avg(INDEX_LOCAL), statistics.getLocalIndexedQueryAvgTime());
      assertEquals(max(INDEX_LOCAL), statistics.getLocalIndexedQueryMaxTime());
      assertEquals(slowestQuery(INDEX_LOCAL), statistics.getSlowestLocalIndexedQuery());

      assertEquals(count(INDEX_DISTRIBUTED), statistics.getDistributedIndexedQueryCount());
      assertEquals(avg(INDEX_DISTRIBUTED), statistics.getDistributedIndexedQueryAvgTime());
      assertEquals(max(INDEX_DISTRIBUTED), statistics.getLocalIndexedQueryMaxTime());
      assertEquals(slowestQuery(INDEX_DISTRIBUTED), statistics.getSlowestDistributedIndexedQuery());

      assertEquals(count(HYBRID), statistics.getHybridQueryCount());
      assertEquals(avg(HYBRID), statistics.getHybridQueryAvgTime());
      assertEquals(max(HYBRID), statistics.getHybridQueryMaxTime());
      assertEquals(slowestQuery(HYBRID), statistics.getSlowestHybridQuery());

      assertEquals(count(NON_INDEXED), statistics.getNonIndexedQueryCount());
      assertEquals(avg(NON_INDEXED), statistics.getNonIndexedQueryAvgTime());
      assertEquals(max(NON_INDEXED), statistics.getNonIndexedQueryMaxTime());
      assertEquals(slowestQuery(NON_INDEXED), statistics.getSlowestNonIndexedQuery());

      assertEquals(SAMPLE_SIZE, statistics.getLoadCount());
   }

   private long count(QueryType queryType) {
      return timePerQuery.entrySet().stream().filter(e -> e.getKey().getType().equals(queryType)).count();
   }

   private double avg(QueryType queryType) {
      return timePerQuery.entrySet().stream()
            .filter(e -> e.getKey().getType().equals(queryType))
            .map(Entry::getValue).collect(Collectors.averagingLong(l -> l));
   }

   private long max(QueryType queryType) {
      return timePerQuery.entrySet().stream()
            .filter(e -> e.getKey().getType().equals(queryType))
            .map(Entry::getValue).max(Long::compareTo).orElse(-1L);
   }

   private String slowestQuery(QueryType queryType) {
      return timePerQuery.entrySet().stream().
            filter(e -> e.getKey().getType().equals(queryType))
            .reduce((e1, e2) -> e1.getValue().compareTo(e2.getValue()) >= 0 ? e1 : e2)
            .map(e -> e.getKey().str).orElse(null);
   }

   static class Query {
      private final QueryType type;
      private final String str;

      public Query(QueryType type, String str) {
         this.type = type;
         this.str = str;
      }

      public QueryType getType() {
         return type;
      }

      public String getStr() {
         return str;
      }

      public static Query random(long q) {
         QueryType[] values = QueryType.values();
         return new Query(values[RANDOM.nextInt(values.length)], String.valueOf(q));
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         Query query = (Query) o;

         if (type != query.type) return false;
         return str.equals(query.str);
      }

      @Override
      public int hashCode() {
         int result = type.hashCode();
         result = 31 * result + str.hashCode();
         return result;
      }

      @Override
      public String toString() {
         return "Query{" +
               "type=" + type +
               ", q='" + str + '\'' +
               '}';
      }
   }

}
