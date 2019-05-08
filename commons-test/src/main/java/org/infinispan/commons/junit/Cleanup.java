package org.infinispan.commons.junit;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.function.Supplier;

import org.jboss.logging.Logger;
import org.junit.rules.ExternalResource;

/**
 * Use with {@link org.junit.Rule @Rule} to release resources after a method
 * or with {@link org.junit.ClassRule @ClassRule} to release them after all the methods in the class.
 *
 * @author Dan Berindei
 * @since 10.0
 */
public class Cleanup extends ExternalResource {
   private static final Logger log = Logger.getLogger(Cleanup.class);
   // TODO Do we need a reference to the test so we can differentiate between resources of subclasses?
   private final Deque<AutoCloseable> stack = new ArrayDeque<>();

   public Cleanup() {
   }

   public Cleanup(AutoCloseable resource) {
      stack.push(resource);
   }

   public Cleanup(AutoCloseable resource1, AutoCloseable resource2) {
      stack.push(resource1);
      stack.push(resource2);
   }

   public Cleanup(Supplier<? extends AutoCloseable> supplier) {
      stack.push(new SupplierCloser(supplier));
   }

   public Cleanup(Supplier<? extends AutoCloseable> supplier1, Supplier<? extends AutoCloseable> supplier2) {
      stack.push(new SupplierCloser(supplier1));
      stack.push(new SupplierCloser(supplier2));
   }

   @Override
   protected void after() {
      while (!stack.isEmpty()) {
         AutoCloseable resource = stack.pop();
         try {
            resource.close();
         } catch (Throwable t) {
            log.errorf(t, "Failed to close resource %s", resource);
         }
      }
   }

   public <T extends AutoCloseable> T add(T resource) {
      stack.push(resource);
      return resource;
   }

   public <T extends AutoCloseable> void add(T resource1, T resource2) {
      stack.push(resource1);
      stack.push(resource2);
   }

   public <T> T add(ExceptionConsumer<T> closer, T resource) {
      stack.push(new NamedCloser<>(closer, resource));
      return resource;
   }

   public <T> void add(ExceptionConsumer<T> closer, T resource1, T resource2) {
      stack.push(new NamedCloser<>(closer, resource1));
      stack.push(new NamedCloser<>(closer, resource2));
   }

   private static class NamedCloser<T> implements AutoCloseable {
      private final ExceptionConsumer<T> closer;
      private final T resource;

      NamedCloser(ExceptionConsumer<T> closer, T resource) {
         this.closer = closer;
         this.resource = resource;
      }

      @Override
      public void close() throws Exception {
         closer.accept(resource);
      }

      @Override
      public String toString() {
         return resource.toString();
      }
   }

   private static class SupplierCloser implements AutoCloseable {
      private final Supplier<? extends AutoCloseable> supplier;

      SupplierCloser(Supplier<? extends AutoCloseable> supplier) {
         this.supplier = supplier;
      }

      @Override
      public void close() throws Exception {
         supplier.get().close();
      }

      @Override
      public String toString() {
         return String.valueOf(supplier.get());
      }
   }

   public interface ExceptionConsumer<T> {
      void accept(T ref) throws Exception;
   }
}
