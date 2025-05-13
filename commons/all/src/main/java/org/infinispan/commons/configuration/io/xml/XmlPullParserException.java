package org.infinispan.commons.configuration.io.xml;

/**
 * This exception is thrown to signal XML Pull Parser related faults.
 *
 * @author <a href="http://www.extreme.indiana.edu/~aslom/">Aleksander Slominski</a>
 */
public class XmlPullParserException extends RuntimeException {
   private final Throwable detail;
   private final int row;
   private final int column;

   public XmlPullParserException(String s) {
      super(s);
      this.detail = null;
      this.row = -1;
      this.column = -1;
   }

   public XmlPullParserException(String msg, XmlPullParser parser, Throwable chain) {
      super((msg == null ? "" : msg + " ")
            + (parser == null ? "" : "(position:" + parser.getPositionDescription() + ") ")
            + (chain == null ? "" : "caused by: " + chain));

      if (parser != null) {
         this.row = parser.getLineNumber();
         this.column = parser.getColumnNumber();
      } else {
         this.row = -1;
         this.column = -1;
      }
      this.detail = chain;
   }

   public Throwable getDetail() {
      return detail;
   }

   public int getLineNumber() {
      return row;
   }

   public int getColumnNumber() {
      return column;
   }
}
