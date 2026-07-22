package org.infinispan.graalvm.server;

import org.infinispan.cli.commands.CredentialsTest;
import org.infinispan.testing.jupiter.tags.Cli;

/**
 * Runs {@link CredentialsTest} against the native CLI binary.
 *
 * @since 16.3
 */
@Cli
public class NativeCredentialsIT extends CredentialsTest {
}
