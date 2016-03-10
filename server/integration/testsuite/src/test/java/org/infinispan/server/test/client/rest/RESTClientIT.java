package org.infinispan.server.test.client.rest;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.server.test.category.RESTSingleNode;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * Test a custom REST client connected to a single Infinispan server.
 * The server is running in standalone mode.
 *
 * @author mgencur
 */
@RunWith(Arquillian.class)
@Category({ RESTSingleNode.class })
public class RESTClientIT extends AbstractRESTClientIT {

    @InfinispanResource("container1")
    RemoteInfinispanServer server1;

    @Override
    protected void addRestServer() {
        RESTHelper.addServer(server1.getRESTEndpoint().getInetAddress().getHostName(), server1.getRESTEndpoint().getContextPath());
    }
}
