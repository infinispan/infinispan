package org.infinispan.server.resp;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.distribution.ch.impl.RESPHashFunctionPartitioner;
import org.infinispan.server.resp.commands.cluster.SegmentSlotRelation;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import io.lettuce.core.cluster.SlotHash;
import io.lettuce.core.codec.CRC16;

@Test(groups = "functional", testName = "server.resp.SlotDistributionTest")
public class SlotDistributionTest {

   private final KeyPartitioner partitioner = RESPHashFunctionPartitioner.instance(16384);
   private final org.infinispan.commons.hash.CRC16 hash = org.infinispan.commons.hash.CRC16.getInstance();

   @Test
   public void testRedisExample() {
      byte[] example = "123456789".getBytes(StandardCharsets.US_ASCII);

      // Example from: https://redis.io/docs/reference/cluster-spec/#appendix-a-crc16-reference-implementation-in-ansi-c
      assertThat(hash.hash(example))
            .isEqualTo(0x31C3);

      // Compare with Lettuce implementation.
      assertThat(hash.hash(example))
            .isEqualTo(CRC16.crc16(example));

      for (int i = 0; i < 1000; i++) {
         byte[] key = ("key" + i).getBytes(StandardCharsets.US_ASCII);
         assertThat(hash.hash(key)).isEqualTo(CRC16.crc16(key));
      }
   }

   @Test
   public void testWithMod() {
      for (int i = 0; i < 1000; i++) {
         byte[] key = ("key" + i).getBytes(StandardCharsets.US_ASCII);
         assertThat(partitioner.getSegment(key)).isEqualTo(hash.hash(key) % 16384);
      }
   }

   @Test(dataProvider = "segments")
   public void testMappingWithLessSegments(int segmentSize) {
      SegmentSlotRelation mapper = new SegmentSlotRelation(segmentSize);
      RESPHashFunctionPartitioner partitioner = RESPHashFunctionPartitioner.instance(segmentSize);
      for (int i = 0; i < 1000; i++) {
         String keyString = "key" + i;
         byte[] key = keyString.getBytes(StandardCharsets.US_ASCII);
         int h = hash.hash(key);
         int segment = partitioner.getSegment(key);
         int slot = SlotHash.getSlot(key);
         assertThat(mapper.hashToSlot(h)).as(keyString).isEqualTo(slot);
         assertThat(mapper.slotToSegment(slot)).as(keyString).isEqualTo(segment);
         assertThat(mapper.segmentToSlots(segment)).as(keyString).contains(slot);
      }
   }

   @DataProvider(name = "segments")
   protected Object[][] segmentsProvider() {
      List<Object[]> segments = new ArrayList<>(14);
      for (int i = 1; i <= 14; i++) {
         segments.add(new Object[]{ 1 << i });
      }

      return segments.toArray(new Object[0][]);
   }

}
