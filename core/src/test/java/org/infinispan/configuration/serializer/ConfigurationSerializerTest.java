package org.infinispan.configuration.serializer;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(testName = "configuration.serializer.ConfigurationSerializerTest", groups="functional")
public class ConfigurationSerializerTest extends AbstractConfigurationSerializerTest {

   @DataProvider(name = "configurationFiles")
   public Object[][] configurationFiles() throws Exception {
      URL configDirURL = Thread.currentThread().getContextClassLoader().getResource("configs/unified");
      Path configDir = Paths.get(configDirURL.toURI());
      return Files.list(configDir)
                  .map(path -> new Object[]{path.subpath(path.getNameCount() - 3, path.getNameCount())})
                  .toArray(Object[][]::new);
   }
}
