package org.infinispan.factories.impl;

import static org.infinispan.commons.test.Exceptions.expectException;
import static org.testng.AssertJUnit.assertNotNull;

import org.infinispan.commons.IllegalLifecycleStateException;
import org.infinispan.factories.ComponentFactory;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.manager.ModuleRepository;
import org.infinispan.manager.TestModuleRepository;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "factories.impl.BasicComponentRegistryImplTest")
public class BasicComponentRegistryImplTest {
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

   public void testRegisterNoFactory() {
      globalRegistry.registerComponent(A.class.getName(), new A(), false);

      // The AA alias doesn't exist at this point
      expectException(RuntimeException.class,
                      () -> globalRegistry.wireDependencies(new B(), true));
      expectException(RuntimeException.class,
                      () -> globalRegistry.registerComponent(B.class.getName(), new B(), false));

      globalRegistry.registerAlias("AA", A.class.getName(), A.class);

      B b1 = new B();
      globalRegistry.wireDependencies(b1, true);
      assertNotNull(b1.a);

      // B is already in the failed state, can't register it again
      expectException(RuntimeException.class,
                      () -> globalRegistry.registerComponent(B.class.getName(), new B(), false));
      // Replace the failed component with a running one
      globalRegistry.replaceComponent(B.class.getName(), b1, true);

      C c1 = new C();
      globalRegistry.wireDependencies(c1, true);
      assertNotNull(c1.a);
      assertNotNull(c1.b);

      C c2 = new C();
      globalRegistry.registerComponent(C.class.getName(), c2, false);
      assertNotNull(c2.a);
      assertNotNull(c2.b);
   }

   public void testRegisterWrongScope() {
      // There's no check that the class scope matches the interface (or superclass) scope yet
      expectException(RuntimeException.class,
                      () -> globalRegistry.registerComponent(D.class.getName(), new D3(), false));
      cacheRegistry.registerComponent(D.class.getName(), new D3(), false);
      expectException(RuntimeException.class,
                      () -> cacheRegistry.registerComponent(H.class.getName(), new H(), false));
      globalRegistry.registerComponent(H.class.getName(), new H(), false);
   }

   @DataProvider(name = "cycleClasses")
   public Object[][] cycleClasses() {
      return new Object[][]{{D.class}, {E.class}, {F.class}};
   }

   @Test(dataProvider = "cycleClasses")
   public void testDependencyCycle(Class<?> entryPoint) {
      cacheRegistry.registerComponent(DEFFactory.class.getName(), new DEFFactory(new D1()), false);

      expectException(RuntimeException.class,
                      () -> cacheRegistry.getComponent(entryPoint));
   }

   public void testDependencyCycleNoStart() {
      cacheRegistry.registerComponent(DEFFactory.class.getName(), new DEFFactory(new D1()), false);

      G g = new G();
      expectException(RuntimeException.class,
                      () -> cacheRegistry.registerComponent(G.class.getName(), g, false));
   }

   @Test(dataProvider = "cycleClasses")
   public void testDependencyCycleLazy(Class<?> entryPoint) {
      cacheRegistry.registerComponent(DEFFactory.class.getName(), new DEFFactory(new D2()), false);

      ComponentRef<?> component = cacheRegistry.getComponent(entryPoint);
      assertNotNull(component);
      Object instance = component.wired();
      assertNotNull(instance);
   }

   public void testDependencyCycleLazyNoStart() {
      registerGlobals();

      cacheRegistry.registerComponent(DEFFactory.class.getName(), new DEFFactory(new D2()), false);

      G g = new G();
      cacheRegistry.registerComponent(G.class.getName(), g, false);

      assertNotNull(g);
      assertNotNull(g.c);
      assertNotNull(g.c.a);
      assertNotNull(g.c.b);
      assertNotNull(g.f.e);
      assertNotNull(g.f.e.d);
      assertNotNull(g.f);
   }

   public void testFactoryRegistersComponent() {
      globalRegistry.registerComponent(HFactory.class.getName(), new HFactory(globalRegistry), false);

      ComponentRef<H> href = globalRegistry.getComponent(H.class.getName(), H.class);
      assertNotNull(href);
      H h = href.wired();
      assertNotNull(h);

      // I is now in the failed state
      ComponentRef<I> iref = globalRegistry.getComponent(I.class);
      expectException(IllegalLifecycleStateException.class, iref::wired);
   }

   private void registerGlobals() {
      globalRegistry.registerComponent(A.class.getName(), new A(), false);
      globalRegistry.registerAlias("AA", A.class.getName(), A.class);
      globalRegistry.registerComponent(B.class.getName(), new B(), false);
      globalRegistry.registerComponent(C.class.getName(), new C(), false);
   }

   @Scope(Scopes.GLOBAL)
   static class A {
   }

   @Scope(Scopes.GLOBAL)
   static class B {
      @ComponentName("AA")
      @Inject A a;
   }

   @Scope(Scopes.GLOBAL)
   static class C {
      A a;
      B b;

      @Inject
      void inject(@ComponentName("AA") A a, B b) {
         this.a = a;
         this.b = b;
      }
   }

   @Scope(Scopes.NAMED_CACHE)
   interface D {
   }

   @Scope(Scopes.NAMED_CACHE)
   static class D1 implements D {
      @Inject F f;
   }

   @Scope(Scopes.NAMED_CACHE)
   static class D2 implements D {
      @Inject ComponentRef<F> f;
   }

   @Scope(Scopes.NAMED_CACHE)
   static class D3 implements D {
   }

   @Scope(Scopes.NAMED_CACHE)
   static class E {
      @ComponentName("DD")
      @Inject D d;
   }

   @Scope(Scopes.NAMED_CACHE)
   static class F {
      @Inject E e;
   }

   @Scope(Scopes.NAMED_CACHE)
   static class G {
      @Inject C c;
      @Inject F f;
   }

   @Scope(Scopes.GLOBAL)
   static class H {
   }

   @Scope(Scopes.GLOBAL)
   static class I {
      @Inject H h;
   }

   @DefaultFactoryFor(classes = {D.class, E.class, F.class}, names = "DD")
   @Scope(Scopes.NAMED_CACHE)
   static class DEFFactory implements ComponentFactory {
      private final D d;

      DEFFactory(D d) {
         this.d = d;
      }

      @Override
      public Object construct(String componentName) {
         if (D.class.getName().equals(componentName)) {
            return d;
         }
         if ("DD".equals(componentName)) {
            return ComponentAlias.of(D.class);
         }
         if (E.class.getName().equals(componentName)) {
            return new E();
         }
         if (F.class.getName().equals(componentName)) {
            return new F();
         }
         throw new UnsupportedOperationException();
      }
   }

   @DefaultFactoryFor(classes = H.class)
   @Scope(Scopes.GLOBAL)
   public static class HFactory implements ComponentFactory {
      final BasicComponentRegistry registry;

      HFactory(BasicComponentRegistry registry) {
         this.registry = registry;
      }

      @Override
      public Object construct(String componentName) {
         if (H.class.getName().equals(componentName)) {
            // Registering I would block waiting on H
            expectException(RuntimeException.class, () -> registry.registerComponent(I.class.getName(), new I(), false));

            H h = new H();
            // Our thread has already started registering H
            expectException(RuntimeException.class, () -> registry.registerComponent(H.class.getName(), h, false));

            // Registering an unrelated component is allowed
            registry.registerComponent("HH", h, false);

            return h;
         }
         throw new UnsupportedOperationException();
      }
   }
}
