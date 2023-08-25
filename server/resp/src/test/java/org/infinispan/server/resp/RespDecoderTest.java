package org.infinispan.server.resp;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;

import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletionStage;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.FixedLengthFrameDecoder;

/**
 * Test to ensure decoder works properly for various requests, types and replay
 *
 * @author William Burns
 * @since 15.0
 */
@Test(groups = "functional", testName = "server.resp.RespDecoderTest")
public class RespDecoderTest {

   EmbeddedChannel channel;
   Queue<Request> queuedCommands;

   static class Request {
      private final RespCommand command;
      private final List<byte[]> arguments;

      Request(RespCommand command, List<byte[]> arguments) {
         this.command = command;
         // Make a copy, because the decoder may reuse the list
         this.arguments = new ArrayList<>(arguments);
      }
   }

   @BeforeClass
   public void beforeClass() {
      queuedCommands = new ArrayDeque<>();
      RespRequestHandler myRespRequestHandler = new RespRequestHandler(null) {
         @Override
         protected CompletionStage<RespRequestHandler> actualHandleRequest(ChannelHandlerContext ctx, RespCommand type, List<byte[]> arguments) {
            queuedCommands.add(new Request(type, arguments));
            return myStage;
         }
      };
      RespDecoder decoder = new RespDecoder();
      channel = new EmbeddedChannel(new FixedLengthFrameDecoder(1), decoder, new RespHandler(decoder, myRespRequestHandler));
   }

   @AfterClass
   public void afterClass() {
      channel.close();
   }

   @Test
   public void testMixtureOfTypes() {
      String commandName = "PSUBSCRIBE";
      String minValueStr = String.valueOf(Long.MIN_VALUE);
      ByteBuf buffer = Unpooled.copiedBuffer("*6\r\n+" + commandName + "\r\n$3\r\nkey\r\n+value\r\n:23\r\n$5\r\nworks\r\n:" + minValueStr + "\r\n", StandardCharsets.US_ASCII);
      channel.writeInbound(buffer);

      channel.checkException();

      Request req = queuedCommands.poll();
      assertNotNull(req);
      assertEquals(commandName, req.command.getName());
      List<byte[]> arguments = req.arguments;
      assertEquals(5, arguments.size());

      assertEquals("key".getBytes(StandardCharsets.US_ASCII), arguments.get(0));
      assertEquals("value".getBytes(StandardCharsets.US_ASCII), arguments.get(1));
      assertEquals("23".getBytes(StandardCharsets.US_ASCII), arguments.get(2));
      assertEquals("works".getBytes(StandardCharsets.US_ASCII), arguments.get(3));
      assertEquals(minValueStr.getBytes(StandardCharsets.US_ASCII), arguments.get(4));

      assertEquals(0, queuedCommands.size());

      assertEquals(0, buffer.readableBytes());
   }
}
