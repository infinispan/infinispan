package org.infinispan.commons.junit;

import java.util.function.Consumer;

import org.jboss.logging.Logger;
import org.junit.rules.ExternalResource;

/**
 * Use with {@link org.junit.ClassRule @ClassRule} to initialize a resource in a non-static method
 * and release it after all the methods in the class.
 *
 * @author Dan Berindei
 * @since 10.0
 */
public class ClassResource<T> extends ExternalResource {
   private static final Logger log = Logger.getLogger(ClassResource.class);
   // TODO Do we need a reference to the test so we can differentiate between resources of subclasses?
   private T resource;
   private Consumer<T> closer;

   public ClassResource() {
      this.closer = r -> {
         try {
            ((AutoCloseable) r).close();
         } catch (Exception e) {
            throw new RuntimeException(e);
         }
      };
   }

   /**
    * Use a custom closer for non-{@linkplain AutoCloseable} resources.
    */
   public ClassResource(Consumer<T> closer) {
      this.closer = closer;
   }

   @Override
   protected void after() {
      try {
         closer.accept(resource);
         resource = null;
      } catch (Throwable t) {
         log.errorf(t, "Failed to close resource %s", resource);
      }
   }

   public T cache(ExceptionSupplier<T> supplier) throws Exception {
      if (resource != null) {
         return resource;
      }

      resource = supplier.get();
      if (!(resource instanceof AutoCloseable)) {
         throw new IllegalStateException(
            "Resource does not implement AutoCloseable, please set up a custom closer in the constructor");
      }

      return resource;
   }

   public T get() {
      return resource;
   }

   public interface ExceptionSupplier<T> {
      T get() throws Exception;
   }
}
