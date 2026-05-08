package org.infinispan.server.router.routes.hotrod;

import static org.junit.jupiter.api.Assertions.assertThrows;

import javax.net.ssl.SSLContext;

import org.junit.jupiter.api.Test;

public class SniNettyRouteSourceTest {

    @Test
    public void shouldValidateSniHostName() throws Exception {
       assertThrows(IllegalArgumentException.class, () -> new SniNettyRouteSource(null, SSLContext.getDefault()).validate());
    }

    @Test
    public void shouldValidateSSLContext() throws Exception {
       assertThrows(IllegalArgumentException.class, () -> new SniNettyRouteSource("test", null).validate());
    }

}
