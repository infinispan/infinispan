package org.infinispan.server.router.routes.rest;

import org.junit.Test;

public class RestRouteSourceTest {

    @Test(expected = IllegalArgumentException.class)
    public void shouldValidatePath() {
        new RestRouteSource(null).validate();
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldValidateWithWhiteCharacters() {
        new RestRouteSource("12312 234").validate();
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldValidateStartingSlash() {
        new RestRouteSource("/test").validate();
    }

    @Test
    public void shouldPassOnCorrectPath() {
        new RestRouteSource("correctPath").validate();
    }
}
