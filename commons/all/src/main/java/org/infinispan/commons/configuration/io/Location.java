package org.infinispan.commons.configuration.io;

/**
 * @since 12.1
 * @author Tristan Tarrant &lt;tristan@infinispan.org%gt;
 */
public interface Location {

  int getLineNumber();

  int getColumnNumber();

  static Location of(int line, int column) {
    return new Location() {
      @Override
      public int getLineNumber() {
        return line;
      }

      @Override
      public int getColumnNumber() {
        return column;
      }

      public String toString() {
        return "[" + line + ',' + column + ']';
      }
    };
  }
}
