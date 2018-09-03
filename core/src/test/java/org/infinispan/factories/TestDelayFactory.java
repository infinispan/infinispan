package org.infinispan.factories;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

/**
 * Internal class, only used for testing.
 *
 * It has to reside in the production source tree because the component metadata persister doesn't parse
 * test classes.
 *
 * @author Dan Berindei
 * @since 5.2
 */
@Scope(Scopes.GLOBAL)
@DefaultFactoryFor(classes = TestDelayFactory.Component.class)
public class TestDelayFactory extends AbstractComponentFactory implements AutoInstantiableFactory {
   private boolean injectionDone = false;
   private Control control;

   @Inject
   public void inject(Control control) throws InterruptedException {
      this.control = control;
      control.await();
      injectionDone = true;
   }

   // Implement the old construct method for testing
   public <T> T construct(Class<T> componentType) {
      if (!injectionDone) {
         throw new IllegalStateException("GlobalConfiguration reference is null");
      }
      return componentType.cast(new Component());
   }

   @Scope(Scopes.GLOBAL)
   public static class Component {
   }

   @Scope(Scopes.GLOBAL)
   public static class Control {
      private final CountDownLatch latch = new CountDownLatch(1);

      public void await() throws InterruptedException {
         latch.await(10, TimeUnit.SECONDS);
      }

      public void unblock() {
         latch.countDown();
      }
   }
}
