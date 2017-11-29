package org.infinispan.server.test.task.servertask;

import static java.util.Comparator.comparing;

import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.infinispan.Cache;
import org.infinispan.commons.dataconversion.IdentityEncoder;
import org.infinispan.tasks.ServerTask;
import org.infinispan.tasks.TaskContext;
import org.infinispan.tasks.TaskExecutionMode;

public class PriceTask implements ServerTask {

   public static final String NAME = "PriceTask";
   public static final String ACTION_PARAM = "action";
   public static final String TICKER_PARAM = "ticker";
   public static final String AVG_PRICE = "avgPrice";
   public static final String TOP_PRICE_ACTION = "topPrice";

   private TaskContext ctx;

   @Override
   public void setTaskContext(TaskContext ctx) {
      this.ctx = ctx;
   }

   @Override
   public String getName() {
      return NAME;
   }

   @SuppressWarnings("unchecked")
   private <E> E getParam(String name) {
      Optional<E> paramOpt = ctx.getParameters().map(m -> (E) m.get(name));
      if (!paramOpt.isPresent()) {
         throw new IllegalArgumentException("Missing 'action' param");
      }
      return paramOpt.get();
   }

   private Predicate<SpotPrice> byTicker(String ticker) {
      return spotPrice -> spotPrice.getTicker().equals(ticker);
   }

   @Override
   public Object call() throws Exception {
      Cache<Integer, SpotPrice> cache = getCache();
      String action = getParam(ACTION_PARAM);
      if (action.equals(AVG_PRICE)) {
         String ticker = getParam(TICKER_PARAM);
         return cache.values().stream()
               .filter(byTicker(ticker))
               .mapToDouble(SpotPrice::getPriceUSD)
               .average().orElseThrow(() -> new IllegalStateException("Cannot calculate average"));
      }

      if (action.equals(TOP_PRICE_ACTION)) {
         String ticker = getParam(TICKER_PARAM);
         return cache.values().stream()
               .filter(byTicker(ticker))
               .sorted(comparing(SpotPrice::getPriceUSD))
               .collect(Collectors.toList());
      }

      throw new IllegalStateException("Invalid or missing 'action' param");
   }

   @Override
   public TaskExecutionMode getExecutionMode() {
      return TaskExecutionMode.ONE_NODE;
   }

   @SuppressWarnings("unchecked")
   private <K, V> Cache<K, V> getCache() {
      return (Cache<K, V>) ctx.getCache().get();
   }

}
