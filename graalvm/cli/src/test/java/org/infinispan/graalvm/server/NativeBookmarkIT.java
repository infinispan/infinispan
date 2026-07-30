package org.infinispan.graalvm.server;

import org.infinispan.cli.commands.BookmarkTest;

/**
 * Runs {@link BookmarkTest} against the native CLI binary.
 * Tests requiring interactive master password prompts or custom
 * connections are skipped in process mode.
 *
 * @since 16.3
 */
public class NativeBookmarkIT extends BookmarkTest {
}
