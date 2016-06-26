package org.infinispan.server.core;

import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;


/**
 * Marshalling test for server core classes.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
@Test(groups = "functional", testName = "server.core.MarshallingTest")
public class MarshallingTest extends AbstractMarshallingTest {
   public void testMarshallingBigByteArrayValue() throws IOException, InterruptedException, ClassNotFoundException {
      byte[] cacheValue = getBigByteArray();
      byte[] bytes = marshaller.objectToByteBuffer(cacheValue);
      byte[] readValue = (byte[]) marshaller.objectFromByteBuffer(bytes);
      AssertJUnit.assertTrue(Arrays.equals(readValue, cacheValue));
   }
}