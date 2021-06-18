package org.infinispan.server.security;

import static org.junit.Assert.assertEquals;

import org.infinispan.commons.util.SslContextFactory;
import org.junit.Test;

public class OpenSSLFactoryTest {

   @Test
   public void testOpenSSLProvider() {
      assertEquals("openssl not installed", "openssl", SslContextFactory.getSslProvider());
   }
}
