package org.infinispan.factories.impl;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.AssertJUnit.assertNotNull;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.infinispan.factories.AutoInstantiableFactory;
import org.infinispan.factories.ComponentFactory;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.manager.ModuleRepository;
import org.infinispan.manager.TestModuleRepository;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "factories.impl.FactoryAutoInstantiationTest")
public class FactoryAutoInstantiationTest extends AbstractInfinispanTest {
   private ModuleRepository moduleRepository;
   private BasicComponentRegistryImpl globalRegistry;
   private BasicComponentRegistryImpl cacheRegistry;

   @BeforeMethod(alwaysRun = true)
   public void setup() {
      moduleRepository = TestModuleRepository.defaultModuleRepository();
      globalRegistry = new BasicComponentRegistryImpl(moduleRepository, true, null);
      cacheRegistry = new BasicComponentRegistryImpl(moduleRepository, false, globalRegistry);
   }

   @AfterMethod(alwaysRun = true)
   public void tearDown() {
      cacheRegistry.stop();
      globalRegistry.stop();
   }

   public void testConcurrentAutoInstantiation() throws Exception {
      globalRegistry.registerComponent(AFactoryDependency.class, new AFactoryDependency(), true);

      int numThreads = 2;
      CyclicBarrier barrier = new CyclicBarrier(numThreads + 1);
      ExecutorService threadPool = Executors.newFixedThreadPool(numThreads, getTestThreadFactory("Worker"));
      ExecutorCompletionService<Object> completionService = new ExecutorCompletionService<>(threadPool);
      for (int i = 0; i < numThreads; i++) {
         completionService.submit(() -> {
            barrier.await(10, SECONDS);
            ComponentRef<AComponent> aRef = cacheRegistry.getComponent(AComponent.class);
            return aRef.wired();
         });
      }
      Thread.sleep(1);
      barrier.await(10, SECONDS);
      threadPool.shutdown();
      for (int i = 0; i < numThreads; i++) {
         Future<Object> future = completionService.poll(111, SECONDS);
         assertNotNull(future);
         assertNotNull(future.get());
      }
   }

   @Scope(Scopes.GLOBAL)
   public static class AComponent {
   }

   @Scope(Scopes.GLOBAL)
   public static class AFactoryDependency{
   }

   @Scope(Scopes.GLOBAL)
   @DefaultFactoryFor(classes = AComponent.class)
   public static class AComponentFactory implements ComponentFactory, AutoInstantiableFactory {
      @Inject
      void inject(AFactoryDependency aDependency) {
         try {
            Thread.sleep(1);
         } catch (InterruptedException e) {
            e.printStackTrace();
         }
      }
      @Override
      public Object construct(String componentName) {
         try {
            Thread.sleep(1);
         } catch (InterruptedException e) {
            e.printStackTrace();
         }
         return new AComponent();
      }
   }
}
