package org.infinispan.spring.remote.provider;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.infinispan.protostream.GeneratedSchema;
import org.testng.annotations.Test;

@Test(testName = "spring.provider.SchemaRegistrationTest", groups = "unit")
public class SchemaRegistrationTest {

   @Test
   public void testDiscoverSchemasWithNoMatchingPackage() {
      List<GeneratedSchema> schemas = SchemaRegistration.discoverSchemas(
            Thread.currentThread().getContextClassLoader(),
            List.of("com.nonexistent.package"));
      assertTrue(schemas.isEmpty());
   }

   @Test
   public void testDiscoverSchemasExcludesInfinispanInternalPackages() {
      // Scanning org.infinispan should return empty because internal packages are filtered
      List<GeneratedSchema> schemas = SchemaRegistration.discoverSchemas(
            Thread.currentThread().getContextClassLoader(),
            List.of("org.infinispan"));
      assertTrue(schemas.isEmpty());
   }

   @Test
   public void testDiscoverSchemasWithEmptyPackageList() {
      List<GeneratedSchema> schemas = SchemaRegistration.discoverSchemas(
            Thread.currentThread().getContextClassLoader(),
            List.of());
      assertTrue(schemas.isEmpty());
   }
}
