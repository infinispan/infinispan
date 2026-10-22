package org.infinispan.client.hotrod.near;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.infinispan.client.hotrod.impl.InvalidatedNearRemoteCache;
import org.testng.annotations.Test;

/**
 * Unit tests for {@link InvalidatedNearRemoteCache.ServerListenerState}.
 */
@Test(groups = "unit", testName = "client.hotrod.near.ServerListenerStateTest")
public class ServerListenerStateTest {

   public void testIncrementVersionIfEven() {
      InvalidatedNearRemoteCache.ServerListenerState state = new InvalidatedNearRemoteCache.ServerListenerState();
      assertThat(state.getVersion()).isEqualTo(0);

      // Even version (0): increment succeeds, version becomes 1
      assertThat(state.incrementVersionIfEven()).isTrue();
      assertThat(state.getVersion()).isEqualTo(1);

      // Odd version (1): increment fails (update in-flight)
      assertThat(state.incrementVersionIfEven()).isFalse();
      assertThat(state.getVersion()).isEqualTo(1);

      // Completion of update: version becomes 2 (even)
      assertThat(state.incrementVersion()).isEqualTo(2);

      // Now even again: increment succeeds, version becomes 3
      assertThat(state.incrementVersionIfEven()).isTrue();
      assertThat(state.getVersion()).isEqualTo(3);

      // Completion: version becomes 4
      assertThat(state.incrementVersion()).isEqualTo(4);
   }

   public void testClusterFailureRecoveryOddVersionsForcedToEven() {
      InvalidatedNearRemoteCache.ServerListenerState state1 = new InvalidatedNearRemoteCache.ServerListenerState();
      InvalidatedNearRemoteCache.ServerListenerState state2 = new InvalidatedNearRemoteCache.ServerListenerState();
      List<InvalidatedNearRemoteCache.ServerListenerState> states = List.of(state1, state2);

      // Both versions transition to odd during update preparation
      assertThat(state1.incrementVersionIfEven()).isTrue();
      assertThat(state2.incrementVersionIfEven()).isTrue();
      assertThat(state1.getVersion()).isEqualTo(1);
      assertThat(state2.getVersion()).isEqualTo(1);

      // Suppose state1 send succeeded (version became 2, even)
      state1.incrementVersion();
      assertThat(state1.getVersion()).isEqualTo(2);

      // State2 send failed (remained 1, odd)
      // When exception handler executes, it must only increment states that are odd:
      states.forEach(s -> {
         if ((s.getVersion() & 1) == 1) {
            s.incrementVersion();
         }
      });

      // Both states must now have even parity, and state1 must not have been advanced to odd
      assertThat(state1.getVersion()).isEqualTo(2);
      assertThat(state1.getVersion() & 1).isEqualTo(0);
      assertThat(state2.getVersion()).isEqualTo(2);
      assertThat(state2.getVersion() & 1).isEqualTo(0);

      // Both states must allow subsequent updates
      assertThat(state1.incrementVersionIfEven()).isTrue();
      assertThat(state2.incrementVersionIfEven()).isTrue();
   }

   public void testAllServersFailRecovery() {
      InvalidatedNearRemoteCache.ServerListenerState state1 = new InvalidatedNearRemoteCache.ServerListenerState();
      InvalidatedNearRemoteCache.ServerListenerState state2 = new InvalidatedNearRemoteCache.ServerListenerState();
      List<InvalidatedNearRemoteCache.ServerListenerState> states = List.of(state1, state2);

      // Both versions transition to odd
      assertThat(state1.incrementVersionIfEven()).isTrue();
      assertThat(state2.incrementVersionIfEven()).isTrue();

      // All servers fail: both are odd
      states.forEach(s -> {
         if ((s.getVersion() & 1) == 1) {
            s.incrementVersion();
         }
      });

      // Both states are recovered to even
      assertThat(state1.getVersion() & 1).isEqualTo(0);
      assertThat(state2.getVersion() & 1).isEqualTo(0);
      assertThat(state1.incrementVersionIfEven()).isTrue();
      assertThat(state2.incrementVersionIfEven()).isTrue();
   }
}
