package org.infinispan.server.test.core;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;

import org.testcontainers.containers.output.BaseConsumer;
import org.testcontainers.containers.output.OutputFrame;

public class CountdownLatchLoggingConsumer extends BaseConsumer<CountdownLatchLoggingConsumer> {

   private CountDownLatch latch;
   private final Pattern pattern;
   private final String regex;

   public CountdownLatchLoggingConsumer(int count, String regex) {
      this.latch = new CountDownLatch(count);
      this.pattern = Pattern.compile(regex, Pattern.DOTALL);
      this.regex = regex;
   }

   @Override
   public void accept(OutputFrame outputFrame) {
      String log = outputFrame.getUtf8String();
      if (pattern.matcher(log).matches()) {
         latch.countDown();
      }
   }

   public void await(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException {
      if (!latch.await(timeout, unit)) {
         throw new TimeoutException(String.format("After the await period %d %s the count down should be 0 and is %d", timeout, unit, latch.getCount()));
      }
   }

   public void resetCountLatch(int count) {
      this.latch = new CountDownLatch(count);

   }
   public String getRegex() {
      return regex;
   }
}
