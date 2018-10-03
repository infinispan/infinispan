package org.infinispan.server.test.client.hotrod;

import org.infinispan.client.hotrod.ProtocolVersion;
import org.infinispan.server.test.category.HotRodSingleNode;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * @since 9.4
 */
@RunWith(Arquillian.class)
@Category(HotRodSingleNode.class)
public class OldClientScriptExecIT extends ScriptExecIT {

   @Override
   protected ProtocolVersion getProtocolVersion() {
      return ProtocolVersion.PROTOCOL_VERSION_25;
   }
}
