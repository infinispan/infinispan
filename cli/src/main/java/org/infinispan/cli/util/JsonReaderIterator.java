package org.infinispan.cli.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

public class JsonReaderIterator implements Iterator<Map<String, String>>, AutoCloseable {
   private final JsonParser parser;
   private Map<String, String> row;
   private boolean eof = false;

   public JsonReaderIterator(Reader reader) throws IllegalArgumentException, IOException {
      Objects.requireNonNull(reader);
      JsonFactory jsonFactory = new JsonFactory();
      this.parser = jsonFactory.createParser((reader instanceof BufferedReader) ? reader : new BufferedReader(reader));
   }

   @Override
   public boolean hasNext() {
      if (eof) {
         return false;
      } else if (row != null) {
         return true;
      } else {
         try {
            while (true) {
               JsonToken token = parser.nextToken();
               if (token == null) {
                  close();
                  return false;
               } else if (token == JsonToken.START_OBJECT) { // List of objects
                  int depth = 0;
                  row = new LinkedHashMap<>();
                  String key = "";
                  while (true) {
                     token = parser.nextToken();
                     if (token == JsonToken.END_OBJECT) {
                        if (depth == 0) {
                           return true;
                        } else {
                           depth--;
                        }
                     } else if (token == JsonToken.START_OBJECT) {
                        depth++;
                     } else if (token == JsonToken.FIELD_NAME) {
                        String name = parser.currentName();
                        token = parser.nextToken();
                        if (token == JsonToken.START_OBJECT) {
                           key = name;
                           depth++;
                        } else {
                           String value = parser.getText();
                           switch (name) {
                              case "_value":
                                 row.put(key, value);
                                 break;
                              case "_type":
                                 break;
                              default:
                                 key = name;
                                 if (value != null) {
                                    row.put(key, value);
                                 }
                           }
                        }
                     }
                  }
               } else if (token == JsonToken.VALUE_STRING) { // List of bare values
                  row = Map.of("", parser.getValueAsString());
                  return true;
               }
            }
         } catch (IOException e) {
            close();
            throw new IllegalStateException(e);
         }
      }
   }

   @Override
   public Map<String, String> next() {
      if (!hasNext()) {
         throw new NoSuchElementException();
      }
      Map<String, String> currentRow = row;
      row = null;
      return currentRow;
   }

   @Override
   public void close() {
      try {
         parser.close();
      } catch (IOException e) {
         // Ignore errors on close
      }
      eof = true;
      row = null;
   }

   @Override
   public void remove() {
      throw new UnsupportedOperationException();
   }

}
