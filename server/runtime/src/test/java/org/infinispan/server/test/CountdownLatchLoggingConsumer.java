package org.infinispan.server.test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.testcontainers.containers.output.BaseConsumer;
import org.testcontainers.containers.output.OutputFrame;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class CountdownLatchLoggingConsumer extends BaseConsumer<CountdownLatchLoggingConsumer> {

   private final CountDownLatch latch;
   private final Pattern pattern;

   public CountdownLatchLoggingConsumer(int count, String regex) {
      this.latch = new CountDownLatch(count);
      this.pattern = Pattern.compile(regex, Pattern.DOTALL);
   }

   @Override
   public void accept(OutputFrame outputFrame) {
      String log = outputFrame.getUtf8String();
      if (pattern.matcher(log).matches()) {
         latch.countDown();
      }
   }

   public void await(long timeout, TimeUnit unit) throws InterruptedException {
      latch.await(timeout, unit);
   }
}
