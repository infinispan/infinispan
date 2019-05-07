package org.infinispan.commons.time;

import com.github.benmanes.caffeine.cache.Ticker;

/**
 * A Ticker for Caffeine backed by a TimeService
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class TimeServiceTicker implements Ticker {

   private final TimeService timeService;

   public TimeServiceTicker(TimeService timeService) {
      this.timeService = timeService;
   }

   @Override
   public long read() {
      return timeService.time();
   }
}
