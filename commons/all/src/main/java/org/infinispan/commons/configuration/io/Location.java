package org.infinispan.commons.configuration.io;

/**
 * @since 12.1
 * @author Tristan Tarrant &lt;tristan@infinispan.org%gt;
 */
public class Location {
  private final String name;
  private final int line;
  private final int column;

  public Location(String name, int line, int column) {
    this.name = name;
    this.line = line;
    this.column = column;
  }

  public String getName() {
    return name;
  }

  public int getLineNumber() {
    return line;
  }

  public int getColumnNumber(){
    return column;
  }

  public String toString() {
    return (name != null ? name : "") + "[" + line + ',' + column + ']';
  }
}
