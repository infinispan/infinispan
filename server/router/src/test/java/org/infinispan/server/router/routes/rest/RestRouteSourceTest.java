package org.infinispan.server.router.routes.rest;

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

public class RestRouteSourceTest {

    @Test
    public void shouldValidatePath() {
       assertThrows(IllegalArgumentException.class, () -> new RestRouteSource(null).validate());
    }

    @Test
    public void shouldValidateWithWhiteCharacters() {
       assertThrows(IllegalArgumentException.class, () -> new RestRouteSource("12312 234").validate());
    }

    @Test
    public void shouldValidateStartingSlash() {
       assertThrows(IllegalArgumentException.class, () -> new RestRouteSource("/test").validate());
    }

    @Test
    public void shouldPassOnCorrectPath() {
        new RestRouteSource("correctPath").validate();
    }
}
