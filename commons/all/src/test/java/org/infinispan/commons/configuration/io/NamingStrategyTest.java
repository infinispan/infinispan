package org.infinispan.commons.configuration.io;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.1
 **/
public class NamingStrategyTest {

   @Test
   public void testNamingStrategyConversion() {
      assertEquals("l1-lifespan", NamingStrategy.KEBAB_CASE.convert("l1Lifespan"));
      assertEquals("thisIsAnIdentifier", NamingStrategy.CAMEL_CASE.convert("this-is-an-identifier"));
      assertEquals("yet-another-identifier", NamingStrategy.KEBAB_CASE.convert("yetAnotherIdentifier"));
      assertEquals("sock_conn_timeout", NamingStrategy.SNAKE_CASE.convert("sockConnTimeout"));
      assertEquals("enabled-ciphersuites-tls13", NamingStrategy.KEBAB_CASE.convert("enabledCiphersuitesTls13"));
   }
}
