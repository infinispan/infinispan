package org.infinispan.factories.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.factories.impl.BasicComponentRegistryImpl.WrapperState;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.time.ControlledTimeService;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "factories.impl.ComponentEntryTest")
public class ComponentEntryTest {

   public void testDurationReturnsMinusOneWithOnlyInstantiating() {
      ControlledTimeService timeService = new ControlledTimeService();
      ComponentEntry entry = new ComponentEntry("comp", WrapperState.EMPTY, null);

      entry.state(WrapperState.INSTANTIATING, timeService);

      assertThat(entry.duration(TimeUnit.MILLISECONDS)).isEqualTo(-1);
   }

   public void testDurationReturnsMinusOneWithoutTimeService() {
      ComponentEntry entry = new ComponentEntry("comp", WrapperState.EMPTY, null);

      entry.state(WrapperState.INSTANTIATING, null);
      entry.state(WrapperState.STARTED, null);

      assertThat(entry.state()).isEqualTo(WrapperState.STARTED);
      assertThat(entry.duration(TimeUnit.MILLISECONDS)).isEqualTo(-1);
   }

   public void testFullLifecycleDuration() {
      ControlledTimeService timeService = new ControlledTimeService();
      ComponentEntry entry = new ComponentEntry("comp", WrapperState.EMPTY, null);

      entry.state(WrapperState.INSTANTIATING, timeService);
      timeService.advance(100);
      entry.state(WrapperState.INSTANTIATED, timeService);
      timeService.advance(200);
      entry.state(WrapperState.WIRING, timeService);
      timeService.advance(300);
      entry.state(WrapperState.WIRED, timeService);
      timeService.advance(400);
      entry.state(WrapperState.STARTING, timeService);
      timeService.advance(500);
      entry.state(WrapperState.STARTED, timeService);

      assertThat(entry.duration(TimeUnit.MILLISECONDS)).isEqualTo(1500);
   }

   public void testSkippedPhasesStillComputesDuration() {
      ControlledTimeService timeService = new ControlledTimeService();
      ComponentEntry entry = new ComponentEntry("comp", WrapperState.EMPTY, null);

      entry.state(WrapperState.INSTANTIATING, timeService);
      timeService.advance(100);
      entry.state(WrapperState.INSTANTIATED, timeService);
      timeService.advance(900);
      entry.state(WrapperState.STARTED, timeService);

      assertThat(entry.duration(TimeUnit.MILLISECONDS)).isEqualTo(1000);
   }

   public void testStateReflectsLatestTransition() {
      ComponentEntry entry = new ComponentEntry("comp", WrapperState.INSTANTIATING, null);

      assertThat(entry.state()).isEqualTo(WrapperState.INSTANTIATING);

      entry.state(WrapperState.WIRED, null);
      assertThat(entry.state()).isEqualTo(WrapperState.WIRED);

      entry.state(WrapperState.STARTED, null);
      assertThat(entry.state()).isEqualTo(WrapperState.STARTED);
   }

   public void testTriggeredByStoresDependencyChain() {
      List<String> deps = List.of("org.example.A", "org.example.B");
      ComponentEntry entry = new ComponentEntry("comp", WrapperState.EMPTY, deps);

      assertThat(entry.triggeredBy()).containsExactly("org.example.A", "org.example.B");
   }

   public void testTriggeredByEmptyWithNullPath() {
      ComponentEntry entry = new ComponentEntry("comp", WrapperState.EMPTY, null);

      assertThat(entry.triggeredBy()).isEmpty();
   }
}
