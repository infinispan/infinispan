package org.infinispan.commons.util.uberjar;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

public class ManifestUberJarDuplicatedJarsWarnerTest {

    @Test
    public void shouldDetectBothUberJars() throws Exception {
        //given
        ManifestUberJarDuplicatedJarsWarner scanner = new ManifestUberJarDuplicatedJarsWarner() {
            @Override
            List<String> getBundleSymbolicNames() {
                return Arrays.asList("org.infinispan.embedded", "org.infinispan.remote");
            }
        };

        //when
        Boolean isClasspathCorrect = scanner.isClasspathCorrect();

        //then
        assertFalse(isClasspathCorrect);
    }

    @Test
    public void shouldDetectCommonsAndUberJar() throws Exception {
        //given
        ManifestUberJarDuplicatedJarsWarner scanner = new ManifestUberJarDuplicatedJarsWarner() {
            @Override
            List<String> getBundleSymbolicNames() {
                return Arrays.asList("org.infinispan.embedded", "org.infinispan.commons");
            }
        };

        //when
        Boolean isClasspathCorrect = scanner.isClasspathCorrect();

        //then
        assertFalse(isClasspathCorrect);
    }

    @Test
    public void shouldNotThrowExceptionOnEmptyClasspath() throws Exception {
        new ManifestUberJarDuplicatedJarsWarner() {
            @Override
            List<String> getBundleSymbolicNames() {
                return Arrays.asList("org.infinispan.embedded", "org.infinispan.commons");
            }
        }.isClasspathCorrect();
    }

    @Test
    public void shouldPassOnNormalClasspath() throws Exception {
        //given
        ManifestUberJarDuplicatedJarsWarner scanner = new ManifestUberJarDuplicatedJarsWarner() {
            @Override
            List<String> getBundleSymbolicNames() {
                return Arrays.asList("org.infinispan.embedded", "org.acme.DonaldDuck");
            }
        };

        //when
        Boolean isClasspathCorrect = scanner.isClasspathCorrect();

        //then
        assertTrue(isClasspathCorrect);
    }

}
