package org.infinispan.graalvm.server;

import org.infinispan.cli.commands.UserTest;

/**
 * Runs {@link UserTest} against the native CLI binary.
 * The {@code infinispan.cli.bin} system property is set by the failsafe
 * configuration in the native profile, which causes {@code CliExtension}
 * to use process mode instead of embedded mode.
 *
 * @since 16.3
 */
public class NativeUserIT extends UserTest {
}
