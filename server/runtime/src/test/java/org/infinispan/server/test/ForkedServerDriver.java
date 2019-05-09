package org.infinispan.server.test;

import java.net.InetSocketAddress;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class ForkedServerDriver extends ServerDriver {

   protected ForkedServerDriver(String name, ServerTestConfiguration configuration) {
      super(name, configuration);
   }

   @Override
   protected void before() {

   }

   @Override
   protected void after() {

   }

   @Override
   public InetSocketAddress getServerAddress(int server, int port) {
      return null;
   }
}
