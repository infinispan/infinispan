package org.infinispan.client.hotrod.near;

import static org.assertj.core.api.Assertions.assertThat;

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
}
