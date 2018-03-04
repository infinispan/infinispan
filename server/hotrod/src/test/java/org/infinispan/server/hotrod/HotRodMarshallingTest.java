package org.infinispan.server.hotrod;

import static org.testng.AssertJUnit.assertEquals;

import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.server.core.AbstractMarshallingTest;
import org.infinispan.util.ByteString;
import org.testng.annotations.Test;

/**
 * Tests marshalling of Hot Rod classes.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
@Test(groups = "functional", testName = "server.hotrod.HotRodMarshallingTest")
public class HotRodMarshallingTest extends AbstractMarshallingTest {

   public void testMarshallingBigByteArrayKey() throws Exception {
      byte[] cacheKey = getBigByteArray();
      byte[] bytes = marshaller.objectToByteBuffer(cacheKey);
      byte[] readKey = (byte[]) marshaller.objectFromByteBuffer(bytes);
      assertEquals(readKey, cacheKey);
   }

   public void testMarshallingCommandWithBigByteArrayKey() throws Exception {
      byte[] cacheKey = getBigByteArray();
      ClusteredGetCommand command =
            new ClusteredGetCommand(new WrappedByteArray(cacheKey), ByteString.fromString("c"), 0, EnumUtil.EMPTY_BIT_SET);
      byte[] bytes = marshaller.objectToByteBuffer(command);
      ClusteredGetCommand readCommand = (ClusteredGetCommand) marshaller.objectFromByteBuffer(bytes);
      assertEquals(readCommand, command);
   }

}
