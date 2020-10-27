package org.infinispan.cli.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ReaderIterator implements Iterator<String>, AutoCloseable {
   private final BufferedReader reader;
   private String line;
   private boolean eof = false;
   private final Pattern regex;
   private Matcher matcher;

   public ReaderIterator(InputStream inputStream, Pattern regex) {
      Objects.requireNonNull(inputStream);
      this.reader = new BufferedReader(new InputStreamReader(inputStream));
      this.regex = regex;
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
               if (regex == null) {
                  // Normal mode
                  String l = reader.readLine();
                  if (l == null) {
                     close();
                     return false;
                  } else {
                     line = l;
                     return true;
                  }
               } else {
                  // Matching mode
                  if (matcher == null) {
                     String l = reader.readLine();
                     if (l == null) {
                        close();
                        return false;
                     }
                     matcher = regex.matcher(l);
                  }
                  if (matcher.find()) {
                     line = matcher.group(1);
                     return true;
                  } else {
                     // Force a new line read
                     matcher = null;
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
         reader.close();
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
