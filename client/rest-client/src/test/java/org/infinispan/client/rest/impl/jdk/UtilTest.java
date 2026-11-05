package org.infinispan.client.rest.impl.jdk;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link Util}.
 */
class UtilTest {

   @Test
   void sanitizeUrlEncodes() {
      assertThat(Util.sanitize("a b/c")).isEqualTo("a+b%2Fc");
   }

   @Test
   void sanitizeKeepsUnreservedCharacters() {
      assertThat(Util.sanitize("abc-123_")).isEqualTo("abc-123_");
   }
}
