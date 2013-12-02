package org.infinispan.server.test.client.rest;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.server.test.category.RESTLocal;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.Inet6Address;

/**
 *
 * @author <a href="mailto:jvilkola@redhat.com">Jozef Vilkolak</a>
 * @author <a href="mailto:mlinhard@redhat.com">Michal Linhard</a>
 * @version November 2013
 */
@RunWith(Arquillian.class)
@Category({ RESTLocal.class })
public class RESTClientTest extends AbstractRESTClientTest {

    @InfinispanResource("container1")
    RemoteInfinispanServer server1;

    @Override
    protected void addRestServer() {
        // IPv6 address should be in square brackets, otherwise http client does not understand it
        if (server1.getRESTEndpoint().getInetAddress() instanceof Inet6Address) {
            RESTHelper.addServer("[" + server1.getRESTEndpoint().getInetAddress().getHostName() + "]", server1.getRESTEndpoint().getContextPath());
        } else { // otherwise should be IPv4
            RESTHelper.addServer(server1.getRESTEndpoint().getInetAddress().getHostName(), server1.getRESTEndpoint().getContextPath());
        }
    }
}
