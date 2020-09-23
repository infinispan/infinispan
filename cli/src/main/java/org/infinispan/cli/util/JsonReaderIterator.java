package org.infinispan.cli.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Predicate;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

public class JsonReaderIterator implements Iterator<String>, AutoCloseable {
   private final Predicate<String> predicate;
   JsonParser parser;
   private String line;
   private boolean eof = false;

   public JsonReaderIterator(Reader reader, Predicate<String> predicate) throws IllegalArgumentException, IOException {
      Objects.nonNull(reader);
      JsonFactory jsonFactory = new JsonFactory();
      this.parser = jsonFactory.createParser((reader instanceof BufferedReader) ? (BufferedReader) reader : new BufferedReader(reader));
      this.predicate = predicate;
   }

   @Override
   public boolean hasNext() {
      if (eof) {
         return false;
      } else if (line != null) {
         return true;
      } else {
         try {
            while (true) {
               JsonToken token = parser.nextToken();
               if (token == null) {
                  close();
                  return false;
               } else if (token == JsonToken.FIELD_NAME) {
                  if (predicate == null || predicate.test(parser.currentName())) {
                     line = parser.nextTextValue();
                     return true;
                  } else {
                     // Discard the value
                     parser.nextTextValue();
                  }
               } else if (token == JsonToken.VALUE_STRING) { // Value with no field
                  if (predicate == null || predicate.test(null)) {
                     line = parser.getValueAsString();
                     return true;
                  }
               }
            }
         } catch (IOException e) {
            close();
            throw new IllegalStateException(e);
         }
      }
   }

   @Override
   public String next() {
      if (!hasNext()) {
         throw new NoSuchElementException();
      }
      String currentLine = line;
      line = null;
      return currentLine;
   }

   @Override
   public void close() {
      try {
         parser.close();
      } catch (IOException e) {
         // Ignore errors on close
      }
      eof = true;
      line = null;
   }

   @Override
   public void remove() {
      throw new UnsupportedOperationException();
   }

}
