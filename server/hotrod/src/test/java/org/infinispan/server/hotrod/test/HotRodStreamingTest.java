package org.infinispan.server.hotrod.test;

import static org.testng.AssertJUnit.assertEquals;

import org.infinispan.server.hotrod.OperationStatus;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "server.hotrod.test.HotRodStreamingTest")
public class HotRodStreamingTest extends org.infinispan.server.hotrod.test.HotRodSingleNodeTest {
   private final byte[] K1 = "K1".getBytes();
   private final byte[] V1;
   private final byte[] V2;
   private final int V1_SIZE = 32_000;
   private final int V2_SIZE = 16_000;

   public HotRodStreamingTest() {
      V1 = fillArray(V1_SIZE);
      V2 = fillArray(V2_SIZE);
   }

   private byte[] fillArray(int size) {
      byte[] array = new byte[size];
      for(int i = 0; i < size; i++) {
         array[i] = (byte)(i % 256);
      }
      return array;
   }

   public void testPutGetStream() {
      TestResponse putResponse = client().putStream(K1, V1, 0, -1, -1);
      assertEquals(OperationStatus.Success, putResponse.getStatus());

      TestGetWithMetadataResponse getResponse = client().getStream(K1, 0);
      assertEquals(-1, getResponse.lifespan);
      assertEquals(-1, getResponse.maxIdle);
      assertEquals(V1, getResponse.data.get());
   }

   public void testPutStreamIfAbsent() {
      TestResponse putResponse = client().putStream(K1, V1, -1, -1, -1);
      assertEquals(OperationStatus.Success, putResponse.getStatus());

      TestGetWithMetadataResponse getResponse = client().getStream(K1, 0);
      assertEquals(-1, getResponse.lifespan);
      assertEquals(-1, getResponse.maxIdle);
      assertEquals(V1, getResponse.data.get());

      putResponse = client().putStream(K1, V2, -1, -1, -1);
      assertEquals(OperationStatus.OperationNotExecuted, putResponse.getStatus());

      getResponse = client().getStream(K1, 0);
      assertEquals(-1, getResponse.lifespan);
      assertEquals(-1, getResponse.maxIdle);
      assertEquals(V1, getResponse.data.get());
   }

   public void testReplaceStream() {
      TestResponse putResponse = client().putStream(K1, V1, -1, -1, -1);
      assertEquals(OperationStatus.Success, putResponse.getStatus());

      TestGetWithMetadataResponse getResponse = client().getStream(K1, 0);
      assertEquals(-1, getResponse.lifespan);
      assertEquals(-1, getResponse.maxIdle);
      assertEquals(V1, getResponse.data.get());

      long k1version = getResponse.dataVersion;

      putResponse = client().putStream(K1, V2, k1version + 3, -1, -1); // use the wrong version
      assertEquals(OperationStatus.OperationNotExecuted, putResponse.getStatus());

      getResponse = client().getStream(K1, 0);
      assertEquals(-1, getResponse.lifespan);
      assertEquals(-1, getResponse.maxIdle);
      assertEquals(V1, getResponse.data.get());

      putResponse = client().putStream(K1, V2, k1version, -1, -1); // use the right version
      assertEquals(OperationStatus.Success, putResponse.getStatus());

      getResponse = client().getStream(K1, 0);
      assertEquals(-1, getResponse.lifespan);
      assertEquals(-1, getResponse.maxIdle);
      assertEquals(V2, getResponse.data.get());


   }

}
