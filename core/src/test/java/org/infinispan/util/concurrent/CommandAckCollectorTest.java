package org.infinispan.util.concurrent;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.IllegalLifecycleStateException;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.impl.ComponentRef;
import org.infinispan.interceptors.distribution.Collector;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.AbstractInfinispanTest;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "util.concurrent.CommandAckCollectorTest")
public class CommandAckCollectorTest extends AbstractInfinispanTest {

   private ScheduledExecutorService executor;
   private CommandAckCollector ackCollector;

   private void init() {
      executor = mock(ScheduledExecutorService.class);
      when(executor.schedule(any(Callable.class), anyLong(), any(TimeUnit.class)))
            .thenReturn(mock(ScheduledFuture.class));
      ackCollector = new CommandAckCollector();
      ackCollector.timeoutExecutor = executor;
      ackCollector.configuration = new ConfigurationBuilder().clustering().remoteTimeout(5000).build();
      ackCollector.start();
   }

   // The timeout task is captured and run by the test, so the outcome doesn't depend on thread scheduling.
   private Callable<?> scheduledTimeout() {
      @SuppressWarnings("unchecked")
      ArgumentCaptor<Callable<Void>> captor = ArgumentCaptor.forClass(Callable.class);
      verify(executor).schedule(captor.capture(), anyLong(), any(TimeUnit.class));
      return captor.getValue();
   }

   public void testTimeoutReportsAcks() throws Exception {
      init();

      Address received = Address.random();
      Address wrongTopology = Address.random();
      Address silent = Address.random();
      Collector<Object> collector = ackCollector.create(1, List.of(received, wrongTopology, silent), 5);
      collector.primaryResult(null, true);

      ackCollector.backupAck(1, received, 0, 5);
      ackCollector.backupAck(1, wrongTopology, 0, 4);
      scheduledTimeout().call();

      assertThatThrownBy(() -> collector.getFuture().get())
            .isInstanceOf(ExecutionException.class)
            .cause().message()
            .contains("ISPN000427", received + "=received",
                  wrongTopology + "=ignored (topology 4, expected 5)", "Installed Topology Id=unknown")
            .doesNotContain(silent + "=");
   }

   public void testTimeoutCompletesWhenComponentIsStopped() throws Exception {
      init();
      @SuppressWarnings("unchecked")
      ComponentRef<DistributionManager> stopped = mock(ComponentRef.class);
      when(stopped.running()).thenThrow(new IllegalLifecycleStateException());
      ackCollector.distributionManager = stopped;

      Collector<Object> collector = ackCollector.create(1, List.of(Address.random()), 5);
      collector.primaryResult(null, true);
      scheduledTimeout().call();

      assertThatThrownBy(() -> collector.getFuture().get())
            .isInstanceOf(ExecutionException.class)
            .cause().message()
            .contains("ISPN000427", "Installed Topology Id=unknown");
   }
}
