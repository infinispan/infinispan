package org.infinispan.reactive;

import static org.testng.AssertJUnit.fail;

import org.infinispan.commons.util.concurrent.CacheBackpressureFullException;
import org.infinispan.test.Exceptions;
import org.testng.annotations.Test;

import io.reactivex.Flowable;
import io.reactivex.schedulers.Schedulers;

@Test(groups = "functional", testName = "reactive.RxJavaPublisherTest")
public class RxJavaPublisherTest {
   public void testExceptionHandling() throws InterruptedException {
      try {
         Flowable.just(new Object())
               .subscribeOn(Schedulers.from(task -> {
                  throw new CacheBackpressureFullException();
               }))
               .subscribe();
         fail("The error should have been thrown from subscribe!");
      } catch (NullPointerException e) {
         Exceptions.assertException(CacheBackpressureFullException.class, e.getCause());
      }
   }
}
