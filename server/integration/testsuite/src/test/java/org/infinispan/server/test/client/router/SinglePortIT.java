package org.infinispan.server.test.client.router;

import static io.netty.buffer.Unpooled.wrappedBuffer;
import static io.netty.handler.codec.http.HttpMethod.POST;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static org.junit.Assert.assertEquals;

import java.util.Queue;

import org.infinispan.arquillian.core.RunningServer;
import org.infinispan.arquillian.core.WithRunningServer;
import org.infinispan.rest.http2.NettyHttpClient;
import org.infinispan.server.test.category.Security;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.CharsetUtil;

/**
 * Tests Single Port feature.
 *
 * Note: This is still a work-in-progress effort!
 *
 * <p>
 *    This testsuite is very far from being completed. Unfortunately at the moment, all we can test is
 *    whether the Endpoint Router is really serving requests. In order to test it more, we need to write
 *    a new Hot Rod client, which will be capable of using both REST and Hot Rod protocol.
 * </p>
 *
 * @author Sebastian Łaskawiec
 * @since 9.3
 */
@RunWith(Arquillian.class)
@Category({Security.class})
@WithRunningServer({@RunningServer(name = "hotrodSslWithSinglePort", config = "testsuite/hotrod-ssl-with-single-port.xml")})
public class SinglePortIT {

   public static final String CACHE_NAME = "default";

   @Test
   public void testRestPlainTextUpgrade() throws Exception {
      //when
      NettyHttpClient client = NettyHttpClient.newHttp2ClientWithHttp11Upgrade();
      client.start("localhost", 8080);

      FullHttpRequest putValueInCacheRequest = new DefaultFullHttpRequest(HTTP_1_1, POST, "/rest/default/test",
            wrappedBuffer("test".getBytes(CharsetUtil.UTF_8)));

      client.sendRequest(putValueInCacheRequest);
      Queue<FullHttpResponse> responses = client.getResponses();

      //then
      assertEquals(1, responses.size());
      assertEquals(HttpResponseStatus.OK, responses.poll().status());
   }

}
