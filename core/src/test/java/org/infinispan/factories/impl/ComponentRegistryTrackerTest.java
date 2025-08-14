package org.infinispan.factories.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.factories.impl.BasicComponentRegistryImpl.WrapperState.INSTANTIATED;
import static org.infinispan.factories.impl.BasicComponentRegistryImpl.WrapperState.STARTING;
import static org.infinispan.factories.impl.BasicComponentRegistryImpl.WrapperState.WIRED;
import static org.infinispan.factories.impl.BasicComponentRegistryImpl.WrapperState.WIRING;

import java.util.concurrent.TimeUnit;

import org.infinispan.commons.time.ControlledTimeService;
import org.infinispan.commons.time.TimeService;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.ModuleRepository;
import org.infinispan.manager.TestModuleRepository;
import org.infinispan.test.TestingUtil;
import org.jgroups.util.Util;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "factories.impl.ComponentRegistryTrackerTest")
public class ComponentRegistryTrackerTest {

   public void testTimeTrackingGlobal() {

      ModuleRepository moduleRepository = TestModuleRepository.defaultModuleRepository();
      BasicComponentRegistryImpl globalRegistry = new BasicComponentRegistryImpl(moduleRepository, true, null);

      ComponentRegistryTracker tracker = ComponentRegistryTracker.timeTracking(globalRegistry, true);
      TestingUtil.replaceField(tracker, "tracker", globalRegistry, BasicComponentRegistryImpl.class);

      ControlledTimeService timeService = new ControlledTimeService();
      globalRegistry.registerComponent(TimeService.class, timeService, false);
      globalRegistry.registerComponent(GlobalConfiguration.class, GlobalConfigurationBuilder.defaultClusteredBuilder().build(), true);

      // The tracker should register steps for all steps.
      try {
         String componentName = "global-component";
         tracker.instantiating(componentName);
         timeService.advance(1_000);

         tracker.instantiated(componentName);
         timeService.advance(1_100);

         tracker.wiring(componentName);
         timeService.advance(1_200);

         tracker.wired(componentName);
         timeService.advance(1_300);

         tracker.starting(componentName);
         timeService.advance(1_400);

         tracker.started(componentName);

         String blame = tracker.dump();

         // Each step took 1s + an increasing amount to distinguish.
         assertThat(blame)
               .contains("instantiating: 1s")
               .contains("instantiated: 1.10s")
               .contains("wiring: 1.20s")
               .contains("wired: 1.30s")
               .contains("starting: 1.40s")
               .contains("total: 6s");

         tracker.clear();
         assertThat(tracker.dump()).isEmpty();
      } finally {
         globalRegistry.stop();
      }
   }

   public void testTimeTrackingCache() {
      ModuleRepository moduleRepository = TestModuleRepository.defaultModuleRepository();
      BasicComponentRegistryImpl globalRegistry = new BasicComponentRegistryImpl(moduleRepository, true, null);

      ControlledTimeService timeService = new ControlledTimeService();
      globalRegistry.registerComponent(TimeService.class, timeService, false);
      BasicComponentRegistryImpl cacheRegistry = new BasicComponentRegistryImpl(moduleRepository, false, globalRegistry);

      ComponentRegistryTracker tracker = ComponentRegistryTracker.timeTracking(cacheRegistry, false);
      TestingUtil.replaceField(tracker, "tracker", globalRegistry, BasicComponentRegistryImpl.class);

      try {
         String componentName = "component-test";
         tracker.instantiating(componentName);
         timeService.advance(1_000);

         tracker.instantiated(componentName);
         timeService.advance(1_100);

         tracker.wiring(componentName);
         timeService.advance(1_200);

         tracker.wired(componentName);
         timeService.advance(1_300);

         tracker.starting(componentName);
         timeService.advance(1_400);

         tracker.started(componentName);

         String blame = tracker.dump();

         // Each step took 1s + an increasing amount to distinguish.
         assertThat(blame)
               .contains("instantiating: 1s")
               .contains("instantiated: 1.10s")
               .contains("wiring: 1.20s")
               .contains("wired: 1.30s")
               .contains("starting: 1.40s")
               .contains("total: 6s");
      } finally {
         globalRegistry.stop();
         cacheRegistry.stop();
      }
   }

   public void testTimeTrackingOrdered() {
      ModuleRepository moduleRepository = TestModuleRepository.defaultModuleRepository();
      BasicComponentRegistryImpl globalRegistry = new BasicComponentRegistryImpl(moduleRepository, true, null);

      ComponentRegistryTracker tracker = ComponentRegistryTracker.timeTracking(globalRegistry, true);
      TestingUtil.replaceField(tracker, "tracker", globalRegistry, BasicComponentRegistryImpl.class);

      ControlledTimeService timeService = new ControlledTimeService();
      globalRegistry.registerComponent(TimeService.class, timeService, false);
      globalRegistry.registerComponent(GlobalConfiguration.class, GlobalConfigurationBuilder.defaultClusteredBuilder().build(), true);

      String componentFast = "fast-component";
      String componentOk = "ok-component";
      String componentSlow = "slow-component";

      // Register the fast component that spent only 500ms total time.
      tracker.instantiating(componentFast);
      timeService.advance(500);
      tracker.started(componentFast);

      // Register ok component with 1s initialization time.
      tracker.instantiating(componentOk);
      timeService.advance(1_000);
      tracker.started(componentOk);

      // Lastly, is the slow component which took 5s to initialize
      tracker.instantiating(componentSlow);
      timeService.advance(5_000);
      tracker.started(componentSlow);

      String dump = tracker.dump();
      assertThat(dump)
            .contains("slow-component", "ok-component", "fast-component");

      int slowIndex = dump.indexOf("slow-component");
      int okIndex = dump.indexOf("ok-component");
      int fastIndex = dump.indexOf("fast-component");

      assertThat(slowIndex).isLessThan(okIndex);
      assertThat(okIndex).isLessThan(fastIndex);
   }

   @Test(dataProvider = "statesToTraverse")
   public void testTimeTrackingSkippingStages(BasicComponentRegistryImpl.WrapperState[] states) {
      ModuleRepository moduleRepository = TestModuleRepository.defaultModuleRepository();
      BasicComponentRegistryImpl globalRegistry = new BasicComponentRegistryImpl(moduleRepository, true, null);

      ComponentRegistryTracker tracker = ComponentRegistryTracker.timeTracking(globalRegistry, true);
      TestingUtil.replaceField(tracker, "tracker", globalRegistry, BasicComponentRegistryImpl.class);

      ControlledTimeService timeService = new ControlledTimeService();
      globalRegistry.registerComponent(TimeService.class, timeService, false);
      globalRegistry.registerComponent(GlobalConfiguration.class, GlobalConfigurationBuilder.defaultClusteredBuilder().build(), true);

      try {

         // All components go through INSTANTIATING.
         String componentName = "skipping-test";
         tracker.instantiating(componentName);
         timeService.advance(1_000);

         for (BasicComponentRegistryImpl.WrapperState state : states) {
            switch (state) {
               case INSTANTIATED -> tracker.instantiated(componentName);
               case WIRING -> tracker.wiring(componentName);
               case WIRED -> tracker.wired(componentName);
               case STARTING -> tracker.starting(componentName);
               default -> {
                  continue;
               }
            }

            timeService.advance(1_000);
         }

         // All components run started once completed.
         tracker.started(componentName);

         assertThat(tracker.dump()).contains("total: " + Util.printTime(1_000 + 1_000 * states.length, TimeUnit.MILLISECONDS));
      } finally {
         globalRegistry.stop();
      }
   }

   @DataProvider(name = "statesToTraverse")
   protected static Object[][] statesToTraverse() {
      return new Object[][] {
            {new BasicComponentRegistryImpl.WrapperState[0]},
            {new BasicComponentRegistryImpl.WrapperState[] { INSTANTIATED} },
            {new BasicComponentRegistryImpl.WrapperState[] { INSTANTIATED, WIRING} },
            {new BasicComponentRegistryImpl.WrapperState[] { INSTANTIATED, WIRING, WIRED} },
            {new BasicComponentRegistryImpl.WrapperState[] { INSTANTIATED, WIRING, WIRED, STARTING} },
            {new BasicComponentRegistryImpl.WrapperState[] { INSTANTIATED, WIRED, STARTING} },
            {new BasicComponentRegistryImpl.WrapperState[] { INSTANTIATED, STARTING} },
            {new BasicComponentRegistryImpl.WrapperState[] { STARTING} },
      };
   }
}
