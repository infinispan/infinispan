package org.infinispan.partitionhandling;

import org.infinispan.Cache;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.distribution.MagicKey;
import org.infinispan.filter.AcceptAllKeyValueFilter;
import org.infinispan.iteration.EntryIterable;
import org.infinispan.iteration.impl.EntryRetriever;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.partionhandling.AvailabilityException;
import org.infinispan.partionhandling.AvailabilityMode;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CheckPoint;
import org.mockito.AdditionalAnswers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.Test;

import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.*;
import static org.testng.AssertJUnit.fail;

/**
 * Tests to make sure that entry retriever pays attention to partition status
 *
 * @author wburns
 * @since 7.0
 */
@Test(groups = "functional", testName = "partitionhandling.EntryRetrieverReplPartitionHandlingTest")
public class EntryRetrieverReplPartitionHandlingTest extends EntryRetrieverDistPartitionHandlingTest {
   public EntryRetrieverReplPartitionHandlingTest() {
      cacheMode = CacheMode.REPL_SYNC;
   }

   @Test(enabled = false)
   public void testUsingIteratorButPartitionOccursBeforeRetrievingRemoteValues() throws InterruptedException {
      // This test is disabled since we don't remotely retrieve values
   }
}
