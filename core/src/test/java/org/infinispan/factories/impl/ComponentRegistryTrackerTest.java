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
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "factories.impl.ComponentRegistryTrackerTest")
public class ComponentRegistryTrackerTest {

   public void testTracksStateAndTiming() {
      ModuleRepository moduleRepository = TestModuleRepository.defaultModuleRepository();
      BasicComponentRegistryImpl globalRegistry = new BasicComponentRegistryImpl(moduleRepository, true, null);

      ControlledTimeService timeService = new ControlledTimeService();
      globalRegistry.registerComponent(TimeService.class, timeService, false);
      globalRegistry.registerComponent(GlobalConfiguration.class,
            GlobalConfigurationBuilder.defaultClusteredBuilder().build(), true);

      BasicComponentRegistryImpl cacheRegistry = new BasicComponentRegistryImpl(moduleRepository, false, globalRegistry);
      DefaultComponentRegistryTracker tracker = new DefaultComponentRegistryTracker(cacheRegistry, false);

      try {
         String name = "org.example.MyComponent";

         tracker.stateChanged(name, BasicComponentRegistryImpl.WrapperState.INSTANTIATING, null);
         timeService.advance(1_000);
         tracker.stateChanged(name, BasicComponentRegistryImpl.WrapperState.INSTANTIATED, null);
         timeService.advance(1_100);
         tracker.stateChanged(name, BasicComponentRegistryImpl.WrapperState.WIRING, null);
         timeService.advance(1_200);
         tracker.stateChanged(name, BasicComponentRegistryImpl.WrapperState.WIRED, null);
         timeService.advance(1_300);
         tracker.stateChanged(name, BasicComponentRegistryImpl.WrapperState.STARTING, null);
         timeService.advance(1_400);
         tracker.stateChanged(name, BasicComponentRegistryImpl.WrapperState.STARTED, null);

         assertThat(tracker.entries()).hasSize(1);

         ComponentEntry entry = tracker.entries().iterator().next();
         assertThat(entry.name()).isEqualTo(name);
         assertThat(entry.state()).isEqualTo(BasicComponentRegistryImpl.WrapperState.STARTED);

         // Total: 1000 + 1100 + 1200 + 1300 + 1400 = 6000ms
         assertThat(entry.duration(TimeUnit.MILLISECONDS)).isEqualTo(6_000);
      } finally {
         cacheRegistry.stop();
         globalRegistry.stop();
      }
   }

   public void testTimeTrackingCache() {
      ModuleRepository moduleRepository = TestModuleRepository.defaultModuleRepository();
      BasicComponentRegistryImpl globalRegistry = new BasicComponentRegistryImpl(moduleRepository, true, null);

      ControlledTimeService timeService = new ControlledTimeService();
      globalRegistry.registerComponent(TimeService.class, timeService, false);
      BasicComponentRegistryImpl cacheRegistry = new BasicComponentRegistryImpl(moduleRepository, false, globalRegistry);

      DefaultComponentRegistryTracker tracker = new DefaultComponentRegistryTracker(cacheRegistry, false);

      try {
         String name = "component-test";

         tracker.stateChanged(name, BasicComponentRegistryImpl.WrapperState.INSTANTIATING, null);
         timeService.advance(1_000);
         tracker.stateChanged(name, BasicComponentRegistryImpl.WrapperState.INSTANTIATED, null);
         timeService.advance(1_100);
         tracker.stateChanged(name, BasicComponentRegistryImpl.WrapperState.WIRING, null);
         timeService.advance(1_200);
         tracker.stateChanged(name, BasicComponentRegistryImpl.WrapperState.WIRED, null);
         timeService.advance(1_300);
         tracker.stateChanged(name, BasicComponentRegistryImpl.WrapperState.STARTING, null);
         timeService.advance(1_400);
         tracker.stateChanged(name, BasicComponentRegistryImpl.WrapperState.STARTED, null);

         assertThat(tracker.entries()).hasSize(1);

         ComponentEntry entry = tracker.entries().iterator().next();
         assertThat(entry.duration(TimeUnit.MILLISECONDS)).isEqualTo(6_000);
      } finally {
         cacheRegistry.stop();
         globalRegistry.stop();
      }
   }

   @Test(dataProvider = "statesToTraverse")
   public void testSkippedPhases(BasicComponentRegistryImpl.WrapperState[] states) {
      ModuleRepository moduleRepository = TestModuleRepository.defaultModuleRepository();
      BasicComponentRegistryImpl globalRegistry = new BasicComponentRegistryImpl(moduleRepository, true, null);

      ControlledTimeService timeService = new ControlledTimeService();
      globalRegistry.registerComponent(TimeService.class, timeService, false);
      globalRegistry.registerComponent(GlobalConfiguration.class,
            GlobalConfigurationBuilder.defaultClusteredBuilder().build(), true);

      BasicComponentRegistryImpl cacheRegistry = new BasicComponentRegistryImpl(moduleRepository, false, globalRegistry);
      DefaultComponentRegistryTracker tracker = new DefaultComponentRegistryTracker(cacheRegistry, false);

      try {
         String name = "skipping-test";

         tracker.stateChanged(name, BasicComponentRegistryImpl.WrapperState.INSTANTIATING, null);
         timeService.advance(1_000);

         for (BasicComponentRegistryImpl.WrapperState state : states) {
            tracker.stateChanged(name, state, null);
            timeService.advance(1_000);
         }

         tracker.stateChanged(name, BasicComponentRegistryImpl.WrapperState.STARTED, null);

         ComponentEntry entry = tracker.entries().iterator().next();
         long expectedMs = 1_000 + 1_000L * states.length;
         assertThat(entry.duration(TimeUnit.MILLISECONDS)).isEqualTo(expectedMs);
      } finally {
         cacheRegistry.stop();
         globalRegistry.stop();
      }
   }

   public void testDependencyChainCapture() {
      ModuleRepository moduleRepository = TestModuleRepository.defaultModuleRepository();
      BasicComponentRegistryImpl globalRegistry = new BasicComponentRegistryImpl(moduleRepository, true, null);

      ControlledTimeService timeService = new ControlledTimeService();
      globalRegistry.registerComponent(TimeService.class, timeService, false);
      globalRegistry.registerComponent(GlobalConfiguration.class,
            GlobalConfigurationBuilder.defaultClusteredBuilder().build(), true);

      BasicComponentRegistryImpl cacheRegistry = new BasicComponentRegistryImpl(moduleRepository, false, globalRegistry);
      DefaultComponentRegistryTracker tracker = new DefaultComponentRegistryTracker(cacheRegistry, false);

      try {
         BasicComponentRegistryImpl.ComponentPath pathA = new BasicComponentRegistryImpl.ComponentPath(
               "org.example.ComponentA", "org.example.ComponentA", null);
         BasicComponentRegistryImpl.ComponentPath pathB = new BasicComponentRegistryImpl.ComponentPath(
               "org.example.ComponentB", "org.example.ComponentB", pathA);
         BasicComponentRegistryImpl.ComponentPath pathCurrent = new BasicComponentRegistryImpl.ComponentPath(
               "org.example.Current", "org.example.Current", pathB);

         tracker.stateChanged("org.example.Current",
               BasicComponentRegistryImpl.WrapperState.INSTANTIATING, pathCurrent);

         ComponentEntry entry = tracker.entries().iterator().next();
         // Head (Current) is skipped; triggered_by is [ComponentB, ComponentA]
         assertThat(entry.triggeredBy())
               .containsExactly("org.example.ComponentB", "org.example.ComponentA");
      } finally {
         cacheRegistry.stop();
         globalRegistry.stop();
      }
   }

   public void testClearAndRemove() {
      ModuleRepository moduleRepository = TestModuleRepository.defaultModuleRepository();
      BasicComponentRegistryImpl globalRegistry = new BasicComponentRegistryImpl(moduleRepository, true, null);

      ControlledTimeService timeService = new ControlledTimeService();
      globalRegistry.registerComponent(TimeService.class, timeService, false);
      globalRegistry.registerComponent(GlobalConfiguration.class,
            GlobalConfigurationBuilder.defaultClusteredBuilder().build(), true);

      BasicComponentRegistryImpl cacheRegistry = new BasicComponentRegistryImpl(moduleRepository, false, globalRegistry);
      DefaultComponentRegistryTracker tracker = new DefaultComponentRegistryTracker(cacheRegistry, false);

      try {
         tracker.stateChanged("comp-a", BasicComponentRegistryImpl.WrapperState.INSTANTIATING, null);
         tracker.stateChanged("comp-b", BasicComponentRegistryImpl.WrapperState.INSTANTIATING, null);
         assertThat(tracker.entries()).hasSize(2);

         tracker.removeComponent("comp-a");
         assertThat(tracker.entries()).hasSize(1);

         tracker.clear();
         assertThat(tracker.entries()).isEmpty();
      } finally {
         cacheRegistry.stop();
         globalRegistry.stop();
      }
   }

   public void testGlobalBootstrapWithoutTimeService() {
      ModuleRepository moduleRepository = TestModuleRepository.defaultModuleRepository();
      BasicComponentRegistryImpl globalRegistry = new BasicComponentRegistryImpl(moduleRepository, true, null);
      DefaultComponentRegistryTracker tracker = new DefaultComponentRegistryTracker(globalRegistry, true);

      try {
         // Before TimeService — state recorded, timing not
         tracker.stateChanged("early-component", BasicComponentRegistryImpl.WrapperState.INSTANTIATING, null);
         tracker.stateChanged("early-component", BasicComponentRegistryImpl.WrapperState.STARTED, null);

         ComponentEntry entry = tracker.entries().iterator().next();
         assertThat(entry.state()).isEqualTo(BasicComponentRegistryImpl.WrapperState.STARTED);
         assertThat(entry.duration(TimeUnit.MILLISECONDS)).isEqualTo(-1);

         // Wire GlobalConfiguration to trigger TimeService resolution
         ControlledTimeService timeService = new ControlledTimeService();
         globalRegistry.registerComponent(TimeService.class, timeService, false);
         tracker.stateChanged(GlobalConfiguration.class.getName(),
               BasicComponentRegistryImpl.WrapperState.WIRED, null);

         // Now timing works
         tracker.stateChanged("late-component", BasicComponentRegistryImpl.WrapperState.INSTANTIATING, null);
         timeService.advance(500);
         tracker.stateChanged("late-component", BasicComponentRegistryImpl.WrapperState.STARTED, null);

         ComponentEntry lateEntry = tracker.entries().stream()
               .filter(e -> e.name().equals("late-component"))
               .findFirst().orElseThrow();
         assertThat(lateEntry.duration(TimeUnit.MILLISECONDS)).isEqualTo(500);
      } finally {
         globalRegistry.stop();
      }
   }

   @DataProvider(name = "statesToTraverse")
   protected static Object[][] statesToTraverse() {
      return new Object[][] {
            {new BasicComponentRegistryImpl.WrapperState[0]},
            {new BasicComponentRegistryImpl.WrapperState[] { INSTANTIATED }},
            {new BasicComponentRegistryImpl.WrapperState[] { INSTANTIATED, WIRING }},
            {new BasicComponentRegistryImpl.WrapperState[] { INSTANTIATED, WIRING, WIRED }},
            {new BasicComponentRegistryImpl.WrapperState[] { INSTANTIATED, WIRING, WIRED, STARTING }},
            {new BasicComponentRegistryImpl.WrapperState[] { INSTANTIATED, WIRED, STARTING }},
            {new BasicComponentRegistryImpl.WrapperState[] { INSTANTIATED, STARTING }},
            {new BasicComponentRegistryImpl.WrapperState[] { STARTING }},
      };
   }
}
