package org.infinispan.commons.util.uberjar;

import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import java.util.jar.Manifest;

import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;

/**
 * Manifest based implementation of a {@link UberJarDuplicatedJarsWarner}.
 * <p>
 *     Incorrect combinations:
 *     <ul>
 *         <li>Commons + any of the Uber Jars</li>
 *         <li>Embedded + Remote Uber Jar</li>
 *         <li>Commons + Embedded + Remote Uber Jar</li>
 *     </ul>
 * </p>
 *
 * @author slaskawi
 */
public class ManifestUberJarDuplicatedJarsWarner implements UberJarDuplicatedJarsWarner {

    private static final Log logger = LogFactory.getLog(MethodHandles.lookup().lookupClass());

    private static final String MANIFEST_LOCATION = "META-INF/MANIFEST.MF";
    private final String SYMBOLIC_NAME_MANIFEST_ENTRY = "Bundle-SymbolicName";

    @Override
    public boolean isClasspathCorrect() {
        List<String> bundleNames = getBundleSymbolicNames();
        long numberOfMatches = bundleNames.stream()
                .filter(hasRemoteUberJar()
                        .or(hasEmbeddedUberJar())
                        .or(hasCommons()))
                .count();
        return numberOfMatches < 2;
    }

    List<String> getBundleSymbolicNames() {
        List<String> symbolicNames = new ArrayList<>();
        try {
            Enumeration<URL> resources = getClass().getClassLoader().getResources(MANIFEST_LOCATION);
            while (resources.hasMoreElements()) {
                URL manifestUrl = resources.nextElement();
                try (InputStream is = manifestUrl.openStream()) {
                    Manifest manifest = new Manifest(is);
                    symbolicNames.add(manifest.getMainAttributes().getValue(SYMBOLIC_NAME_MANIFEST_ENTRY));
                }
            }
        } catch (IOException e) {
            logger.warn("Can not extract jar manifest from the classpath. Uber Jar classpath check is skipped.");
        }
        return symbolicNames;
    }

    Predicate<String> hasCommons() {
        return jarSymbolicName -> "org.infinispan.commons".equals(jarSymbolicName);
    }

    Predicate<String> hasEmbeddedUberJar() {
        return jarSymbolicName -> "org.infinispan.embedded".equals(jarSymbolicName);
    }

    Predicate<String> hasRemoteUberJar() {
        return jarSymbolicName -> "org.infinispan.remote".equals(jarSymbolicName);
    }

    @Override
    public CompletableFuture<Boolean> isClasspathCorrectAsync() {
        return CompletableFuture.supplyAsync(() -> isClasspathCorrect());
    }
}
