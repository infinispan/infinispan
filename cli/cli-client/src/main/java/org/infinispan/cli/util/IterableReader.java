package org.infinispan.cli.util;

import java.io.InputStream;
import java.util.Iterator;
import java.util.regex.Pattern;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class IterableReader implements Iterable<String> {

   private final ReaderIterator iterator;

   public IterableReader(InputStream is, Pattern regex) {
      this.iterator = new ReaderIterator(is, regex);
   }

   @Override
   public Iterator<String> iterator() {
      return iterator;
   }
}
