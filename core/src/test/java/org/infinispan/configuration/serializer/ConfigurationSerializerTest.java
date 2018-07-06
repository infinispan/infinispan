package org.infinispan.configuration.serializer;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(testName = "configuration.serializer.ConfigurationSerializerTest", groups="functional")
public class ConfigurationSerializerTest extends AbstractConfigurationSerializerTest {

   @DataProvider(name = "configurationFiles")
   public Object[][] configurationFiles() throws Exception {
      URL configDir = Thread.currentThread().getContextClassLoader().getResource("configs/unified");
      List<Path> paths = Files.list(Paths.get(configDir.toURI())).collect(Collectors.toList());
      Object[][] configurationFiles = new Object[paths.size()][];
      for (int i = 0; i < paths.size(); i++) {
         configurationFiles[i] = new Object[]{paths.get(i)};
      }
      return configurationFiles;
   }
}
