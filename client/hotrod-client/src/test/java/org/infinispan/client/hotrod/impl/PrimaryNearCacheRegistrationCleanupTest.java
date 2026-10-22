package org.infinispan.client.hotrod.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.client.hotrod.impl.PrimaryNearCacheTestFixture.A;
import static org.infinispan.client.hotrod.impl.PrimaryNearCacheTestFixture.B;
import static org.infinispan.client.hotrod.impl.PrimaryNearCacheTestFixture.hash;
import static org.infinispan.client.hotrod.impl.PrimaryNearCacheTestFixture.info;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;

import org.testng.annotations.Test;

@Test(groups = "unit", testName = "client.hotrod.impl.PrimaryNearCacheRegistrationCleanupTest")
public class PrimaryNearCacheRegistrationCleanupTest {
   public void testRegistrationCompletingAfterCleanupIsRemoved() {
      var f = new PrimaryNearCacheTestFixture(true);
      f.start(A);
      CompletableFuture<Void> bloomUpdate = new CompletableFuture<>();
      when(f.dispatcher.executeOnSingleAddress(f.updateOp, B)).thenReturn(bloomUpdate);

      f.accept(info(hash(A, B)));
      verify(f.dispatcher).executeOnSingleAddress(f.updateOp, B);
      // Remove the listener while a new owner's registration waits for its initial Bloom update.
      f.cache.removeClientListener(f.listener);
      assertThat(f.cache.isPrimaryMode()).isFalse();

      bloomUpdate.complete(null);

      assertThat(f.cache.hasActivePrimaryListeners()).isFalse();
      verify(f.dispatcher).executeOnSingleAddress(f.removeOp, B);
   }
}
