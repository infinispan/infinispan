package org.infinispan.cli.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Iterator;
import java.util.function.Predicate;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class IterableJsonReader implements Iterable<String> {

   private final JsonReaderIterator iterator;

   public IterableJsonReader(InputStream is, Predicate<String> predicate) throws IOException {
      this(new InputStreamReader(is), predicate);
   }

   public IterableJsonReader(Reader r, Predicate<String> predicate) throws IOException {
      this.iterator = new JsonReaderIterator(r, predicate);
   }

   @Override
   public Iterator<String> iterator() {
      return iterator;
   }
}
