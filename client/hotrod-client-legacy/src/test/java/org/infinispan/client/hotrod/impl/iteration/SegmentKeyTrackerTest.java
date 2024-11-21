package org.infinispan.client.hotrod.impl.iteration;

import static org.infinispan.client.hotrod.impl.protocol.HotRodConstants.NO_ERROR_STATUS;
import static org.testng.AssertJUnit.assertTrue;

import java.net.SocketAddress;
import java.util.Set;

import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.impl.consistenthash.SegmentConsistentHash;
import org.infinispan.commons.configuration.ClassAllowList;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.marshall.IdentityMarshaller;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "client.hotrod.SegmentKeyTrackerTest")
public class SegmentKeyTrackerTest {

   public void testNoDuplication() {
      SocketAddress[][] segmentOwners = new SocketAddress[0][0];
      SegmentConsistentHash ch = new SegmentConsistentHash();
      ch.init(segmentOwners, 2);
      DataFormat df = DataFormat.builder()
            .keyType(MediaType.APPLICATION_OBJECT)
            .keyMarshaller(IdentityMarshaller.INSTANCE)
            .build();

      KeyTracker tracker = new SegmentKeyTracker(df, ch, Set.of(0, 1));

      // Belongs to segment 0
      byte[] key = new byte[] { 0b0, 0b1 };
      IntSet completed = IntSets.from(Set.of(0));

      assertTrue(tracker.track(key, NO_ERROR_STATUS, new ClassAllowList()));
      tracker.segmentsFinished(completed);
      Exceptions.expectException(IllegalStateException.class, () ->
            tracker.track(key, NO_ERROR_STATUS, new ClassAllowList()));

      // Belongs to another segment.
      byte[] anotherKey = new byte[] { 0b1, 0b0, 0b1 };
      assertTrue(tracker.track(anotherKey, NO_ERROR_STATUS, new ClassAllowList()));
   }
}
