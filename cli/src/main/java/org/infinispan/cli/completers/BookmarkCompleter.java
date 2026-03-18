package org.infinispan.cli.completers;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Properties;
import java.util.TreeSet;

import org.infinispan.cli.Context;
import org.infinispan.cli.commands.Bookmark;

/**
 * @since 16.2
 */
public class BookmarkCompleter extends ListCompleter {
   @Override
   Collection<String> getAvailableItems(Context context) throws IOException {
      Path bookmarksFile = context.configPath().resolve(Bookmark.BOOKMARKS_FILE);
      TreeSet<String> names = new TreeSet<>();
      if (Files.exists(bookmarksFile)) {
         Properties props = new Properties();
         try (Reader r = Files.newBufferedReader(bookmarksFile)) {
            props.load(r);
         }
         for (String key : props.stringPropertyNames()) {
            int dot = key.indexOf('.');
            if (dot > 0) {
               names.add(key.substring(0, dot));
            }
         }
      }
      return names;
   }
}
