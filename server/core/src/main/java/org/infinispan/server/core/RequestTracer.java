package org.infinispan.server.core;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;

import io.opentracing.Span;
import io.opentracing.Tracer;

public class RequestTracer {
   private static final Log log = LogFactory.getLog(RequestTracer.class);
   private static TracerAdapter adapter;

   public static final String TRACER_FACTORY_CLASS = System.getProperty("infinispan.opentracing.factory.class");

   public static final String TRACER_FACTORY_METHOD = System.getProperty("infinispan.opentracing.factory.method");

   static {
      if (TRACER_FACTORY_CLASS != null && TRACER_FACTORY_METHOD != null) {
         try {
            Class<?> tracerFactoryClass =
                  Thread.currentThread().getContextClassLoader().loadClass(RequestTracer.TRACER_FACTORY_CLASS);
            Method getTracer = tracerFactoryClass.getMethod(TRACER_FACTORY_METHOD);
            Object factoryInstance;
            if (Modifier.isStatic(getTracer.getModifiers())) {
               factoryInstance = null;
            } else {
               factoryInstance = tracerFactoryClass.getDeclaredConstructor().newInstance();
            }
            adapter = new TracerAdapter((Tracer) getTracer.invoke(factoryInstance));
            log.infof("OpenTracing implementation loaded: %s", adapter);
         } catch (Throwable e) {
            log.warnf(e, "OpenTracing implementation could not be loaded");
         }
      } else {
         log.info("OpenTracing integration is disabled");
      }
   }

   public static void start() {
      // start() method call is only needed to trigger the static initializer
   }

   public static void stop() {
      if (adapter != null) {
         adapter.stop();
      }
   }

   public static Object requestStart(String operationName) {
      if (adapter != null) {
         return adapter.requestStart(operationName);
      }
      return null;
   }

   public static void requestEnd(Object span) {
      if (adapter != null) {
         adapter.requestEnd(span);
      }
   }

   public static class TracerAdapter {
      private Tracer tracer;

      public TracerAdapter(Tracer tracer) {
         this.tracer = tracer;
      }

      public void stop() {
         if (tracer instanceof AutoCloseable) {
            try {
               ((AutoCloseable) tracer).close();
            } catch (Exception e) {
               log.warn("OpenTracing error during stop", e);
            }
         }
      }

      public Object requestStart(String operationName) {
         return tracer.buildSpan(operationName).start();
      }

      public void requestEnd(Object span) {
         if (span instanceof Span) {
            ((Span) span).finish();
         }
      }

      @Override
      public String toString() {
         return tracer.toString();
      }
   }
}
