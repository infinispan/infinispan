package org.infinispan.quarkus.embedded;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;

import org.infinispan.commons.util.Util;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;

public class InfinispanEmbeddedTestResource implements QuarkusTestResourceLifecycleManager {
    @Override
    public Map<String, String> start() {
        String xmlLocation = Paths.get("src", "main", "resources", "embedded.xml").toString();
        return Collections.singletonMap("quarkus.infinispan-embedded.xml-config", xmlLocation);
    }

    @Override
    public void stop() {
        // Need to clean up persistent file - so tests dont' leak between each other
        String tmpDir = System.getProperty("java.io.tmpdir");
        try (Stream<Path> files = Files.walk(Paths.get(tmpDir), 1)) {
            files.filter(Files::isDirectory)
                    .filter(p -> p.getFileName().toString().startsWith("quarkus-"))
                    .map(Path::toFile)
                    .forEach(Util::recursiveFileRemove);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
