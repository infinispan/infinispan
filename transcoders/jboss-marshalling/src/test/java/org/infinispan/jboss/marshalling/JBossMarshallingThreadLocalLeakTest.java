package org.infinispan.jboss.marshalling;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.util.ThreadLocalLeakTest;
import org.testng.annotations.Test;

/**
 * AbstractJBossMarshaller uses a thread-local cache of RiverMarshaller instances,
 * so it's worth checking for thread leaks.
 *
 * @author Dan Berindei
 * @since 13.0
 */
@Test(groups = "functional", testName = "jboss.marshalling.JBossMarshallingThreadLocalLeakTest")
public class JBossMarshallingThreadLocalLeakTest extends ThreadLocalLeakTest {
   @Override
   protected void amendConfiguration(ConfigurationBuilder builder) {
      builder.encoding().mediaType(MediaType.APPLICATION_JBOSS_MARSHALLING_TYPE);
   }
}
