package org.infinispan.telemetry.jfr;

import org.infinispan.telemetry.InfinispanSpan;
import org.infinispan.telemetry.SafeAutoClosable;

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Event;
import jdk.jfr.Label;
import jdk.jfr.StackTrace;

@Label("Infinispan Event")
@Description("Infinispan Tracing Events")
@Category("infinispan-internal")
@StackTrace(value = false)
public class JfrSpan<T> extends Event implements InfinispanSpan<T>, SafeAutoClosable {

   @Label("Operation")
   public final String operation;
   @Label("CacheName")
   public final String cacheName;
   @Label("Category")
   public final String category;
   @Label("Exception")
   public String exception;

   public JfrSpan(String operation, String cacheName, String category) {
      this.operation = operation;
      this.cacheName = cacheName;
      this.category = category;
   }

   @Override
   public SafeAutoClosable makeCurrent() {
      return this;
   }

   @Override
   public void complete() {
      end();
      if (shouldCommit()) {
         commit();
      }
   }

   @Override
   public void recordException(Throwable throwable) {
      exception = throwable.getLocalizedMessage();
   }

   @Override
   public void close() {
      //no-op
   }
}
