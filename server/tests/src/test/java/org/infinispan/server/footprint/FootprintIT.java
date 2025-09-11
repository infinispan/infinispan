package org.infinispan.server.footprint;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.stream.Stream;

import javax.management.JMException;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;

import org.infinispan.server.test.api.TestUser;
import org.infinispan.server.test.core.ServerRunMode;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.infinispan.server.test.junit5.InfinispanServerExtensionBuilder;
import org.infinispan.util.logging.Log;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 15.0
 **/
public class FootprintIT {
   private static final int LOADED_CLASS_COUNT_LOWER_BOUND = 11_700;
   private static final int LOADED_CLASS_COUNT_UPPER_BOUND = 11_900;
   private static final long HEAP_USAGE_LOWER_BOUND = 25_000_000L;
   private static final long HEAP_USAGE_UPPER_BOUND = 28_500_000L;
   private static final long DISK_USAGE_LOWER_BOUND = 76_000_000L;
   private static final long DISK_USAGE_UPPER_BOUND = 77_500_000L;

   private static final int JACOCO_CLASS_COUNT = 165;
   private static final long JACOCO_HEAP_USAGE = 2_350_000L;

   private static final int INSIGHTS_CLASS_COUNT = 90;
   private static final int INSIGHTS_HEAP_USAGE = 150_000;
   private static final long INSIGHTS_DISK_USAGE = 250_000;

   public static final String HEAP_DUMP = "footprint.hprof";

   @RegisterExtension
   public static InfinispanServerExtension SERVERS =
         InfinispanServerExtensionBuilder.config("configuration/Footprint.xml")
               .runMode(ServerRunMode.CONTAINER)
               .numServers(1)
               .enableJMX()
               .build();

   @Test
   public void testMemoryFootprint() throws JMException, IOException {
      MBeanServerConnection jmxConnection = SERVERS.jmx().withCredentials(TestUser.ADMIN.getUser(), TestUser.ADMIN.getPassword()).get(0);
      int loadedClassCount = (Integer) jmxConnection.getAttribute(new ObjectName("java.lang:type=ClassLoading"), "LoadedClassCount");
      ObjectName memory = new ObjectName("java.lang:type=Memory");
      jmxConnection.invoke(memory, "gc", new Object[0], new String[0]);
      CompositeData heapMemoryUsage = (CompositeData) jmxConnection.getAttribute(memory, "HeapMemoryUsage");
      Long usedHeap = (Long) heapMemoryUsage.get("used");
      int classCountOffset = 0;
      long heapCountOffset = 0;
      if (Boolean.getBoolean("coverage.enabled")) {
         classCountOffset += JACOCO_CLASS_COUNT;
         heapCountOffset += JACOCO_HEAP_USAGE;
      }
      if (Boolean.getBoolean("insights.enabled")) {
         classCountOffset += INSIGHTS_CLASS_COUNT;
         heapCountOffset += INSIGHTS_HEAP_USAGE;
      }
      try {
         Log.CONTAINER.infof("Loaded classes: %d (offset = %d)", loadedClassCount, classCountOffset);
         Log.CONTAINER.infof("Used heap: %d (offset = %d)", usedHeap, heapCountOffset);
         assertThat(loadedClassCount - classCountOffset).as("Loaded class count").isBetween(LOADED_CLASS_COUNT_LOWER_BOUND, LOADED_CLASS_COUNT_UPPER_BOUND);
         assertThat(usedHeap - heapCountOffset).as("Heap memory usage").isBetween(HEAP_USAGE_LOWER_BOUND, HEAP_USAGE_UPPER_BOUND);
      } catch (AssertionError e) {
         ObjectName hotSpot = new ObjectName("com.sun.management:type=HotSpotDiagnostic");
         jmxConnection.invoke(hotSpot, "dumpHeap", new Object[]{HEAP_DUMP, true}, new String[]{"java.lang.String", "boolean"});
         String s = SERVERS.getServerDriver().syncFilesFromServer(0, "/opt/infinispan/" + HEAP_DUMP);
         Path dump = Paths.get(System.getProperty("build.directory"), HEAP_DUMP);
         Files.move(Paths.get(s, HEAP_DUMP), dump, StandardCopyOption.REPLACE_EXISTING);
         Log.CONTAINER.warnf("Exported heap dump to %s", dump);
         throw e;
      }
   }

   @Test
   public void testDiskFootprint() throws IOException {
      long diskCountOffset = 0;
      if (Boolean.getBoolean("insights.enabled")) {
         diskCountOffset += INSIGHTS_DISK_USAGE;
      }
      Path folder = Paths.get(System.getProperty("org.infinispan.test.server.dir"));
      try (Stream<Path> stream = Files.walk(folder)) {
         long size = stream.filter(p -> p.toFile().isFile())
               .mapToLong(p -> p.toFile().length())
               .sum();
         assertThat(size - diskCountOffset).as("Disk footprint").isBetween(DISK_USAGE_LOWER_BOUND, DISK_USAGE_UPPER_BOUND);
      }
   }
}
