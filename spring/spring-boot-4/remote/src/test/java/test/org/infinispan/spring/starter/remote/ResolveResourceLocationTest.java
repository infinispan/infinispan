package test.org.infinispan.spring.starter.remote;

import static org.assertj.core.api.Assertions.assertThat;

import org.infinispan.spring.starter.remote.InfinispanRemoteAutoConfiguration;
import org.junit.jupiter.api.Test;

/**
 * Verifies that {@link InfinispanRemoteAutoConfiguration#resolveResourceLocation(String)}
 * normalizes resource paths so that bare filenames are always resolved from the classpath,
 * even in a {@code WebApplicationContext} where unprefixed paths would otherwise resolve
 * relative to the servlet context root.
 *
 * @see <a href="https://docs.spring.io/spring-framework/reference/core/resources.html">
 *      Spring Resources documentation</a>
 */
public class ResolveResourceLocationTest {

   @Test
   void bareFilenameGetsClasspathPrefix() {
      assertThat(InfinispanRemoteAutoConfiguration.resolveResourceLocation("hotrod-client.properties"))
            .isEqualTo("classpath:hotrod-client.properties");
   }

   @Test
   void classpathPrefixIsPreserved() {
      assertThat(InfinispanRemoteAutoConfiguration.resolveResourceLocation("classpath:hotrod-client.properties"))
            .isEqualTo("classpath:hotrod-client.properties");
   }

   @Test
   void filePrefixIsPreserved() {
      assertThat(InfinispanRemoteAutoConfiguration.resolveResourceLocation("file:/opt/config/hotrod-client.properties"))
            .isEqualTo("file:/opt/config/hotrod-client.properties");
   }

   @Test
   void httpPrefixIsPreserved() {
      assertThat(InfinispanRemoteAutoConfiguration.resolveResourceLocation("https://config.example.com/hotrod-client.properties"))
            .isEqualTo("https://config.example.com/hotrod-client.properties");
   }

   @Test
   void relativePathGetsClasspathPrefix() {
      assertThat(InfinispanRemoteAutoConfiguration.resolveResourceLocation("config/hotrod-client.properties"))
            .isEqualTo("classpath:config/hotrod-client.properties");
   }

   @Test
   void absolutePathIsPreserved() {
      assertThat(InfinispanRemoteAutoConfiguration.resolveResourceLocation("/opt/config/hotrod-client.properties"))
            .isEqualTo("/opt/config/hotrod-client.properties");
   }

   @Test
   void defaultPropertyAlreadyHasClasspathPrefix() {
      String defaultProp = "classpath:hotrod-client.properties";
      assertThat(InfinispanRemoteAutoConfiguration.resolveResourceLocation(defaultProp))
            .isEqualTo(defaultProp);
   }
}
