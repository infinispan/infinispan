package org.infinispan.jupiter;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

public class SampleTest extends AbstractInfinispanTest {

   @Test
   public void test1() throws Exception {
      System.out.println("test1");
      Future<String> future = fork(() -> "works");
      String msg = future.get(10, TimeUnit.SECONDS);
      System.out.println(msg);
   }

   @Test
   public void test2() {

   }
}
