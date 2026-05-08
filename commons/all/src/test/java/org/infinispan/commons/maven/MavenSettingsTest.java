package org.infinispan.commons.maven;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.jupiter.api.Test;

public class MavenSettingsTest {

   @Test
   public void testMavenSettings() {
      Path path = Paths.get(System.getProperty("build.directory"), "test-classes", "custom-maven-settings.xml");
      MavenSettings s1 = MavenSettings.init(path);
      MavenSettings s2 = MavenSettings.init();
      assertSame(s1, s2);
      assertEquals(Paths.get(System.getProperty("build.directory"), "custom-maven-repo"), s1.getLocalRepository());
   }

}
