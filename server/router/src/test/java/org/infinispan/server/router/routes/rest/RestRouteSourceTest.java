package org.infinispan.server.router.routes.rest;

import org.junit.Test;

public class RestRouteSourceTest {

    @Test(expected = IllegalArgumentException.class)
    public void shouldValidatePath() throws Exception {
        new RestRouteSource(null).validate();
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldValidateWithWhiteCharacters() throws Exception {
        new RestRouteSource("12312 234").validate();
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldValidateStartingSlash() throws Exception {
        new RestRouteSource("/test").validate();
    }

    @Test
    public void shouldPassOnCorrectPath() throws Exception {
        new RestRouteSource("correctPath").validate();
    }
}
