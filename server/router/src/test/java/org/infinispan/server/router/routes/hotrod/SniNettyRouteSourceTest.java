package org.infinispan.server.router.routes.hotrod;

import javax.net.ssl.SSLContext;

import org.junit.Test;

public class SniNettyRouteSourceTest {

    @Test(expected = IllegalArgumentException.class)
    public void shouldValidateSniHostName() throws Exception {
        new SniNettyRouteSource(null, SSLContext.getDefault()).validate();
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldValidateSSLContext() throws Exception {
        new SniNettyRouteSource("test", null).validate();
    }

}
