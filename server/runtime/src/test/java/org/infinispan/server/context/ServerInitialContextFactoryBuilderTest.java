package org.infinispan.server.context;

import java.util.Properties;

import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.spi.InitialContextFactory;

import org.junit.Test;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 13.0
 **/
public class ServerInitialContextFactoryBuilderTest {

   @Test
   public void testNoIllegalAccessJDKInitialContextFactories() throws NamingException {
      ServerInitialContextFactoryBuilder builder = new ServerInitialContextFactoryBuilder();
      Properties env = new Properties();
      env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.dns.DnsContextFactory");
      InitialContextFactory contextFactory = builder.createInitialContextFactory(env);
   }
}
