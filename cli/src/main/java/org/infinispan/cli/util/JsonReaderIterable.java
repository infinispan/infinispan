package org.infinispan.cli.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Iterator;
import java.util.Map;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class JsonReaderIterable implements Iterable<Map<String, String>> {

   private final JsonReaderIterator iterator;

   public JsonReaderIterable(InputStream is) throws IOException {
      this(new InputStreamReader(is));
   }

   public JsonReaderIterable(Reader r) throws IOException {
      this.iterator = new JsonReaderIterator(r);
   }

   @Override
   public Iterator<Map<String, String>> iterator() {
      return iterator;
   }
}
