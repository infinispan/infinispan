package org.infinispan.server.core.dataconversion.xml;

import java.io.IOException;
import java.io.Reader;

import org.infinispan.commons.configuration.io.xml.MXParser;

import com.thoughtworks.xstream.converters.ErrorWriter;
import com.thoughtworks.xstream.io.StreamException;
import com.thoughtworks.xstream.io.naming.NameCoder;
import com.thoughtworks.xstream.io.xml.AbstractPullReader;
import com.thoughtworks.xstream.io.xml.XmlFriendlyNameCoder;

public class MXParserReader extends AbstractPullReader {
   private final MXParser parser;
   private final Reader reader;

   public MXParserReader(Reader reader, MXParser parser) {
      this(reader, parser, new XmlFriendlyNameCoder());
   }

   public MXParserReader(Reader reader, MXParser parser, NameCoder nameCoder) {
      super(nameCoder);
      this.parser = parser;
      this.reader = reader;

      try {
         parser.setInput(this.reader);
      } catch (Exception e) {
         throw new StreamException(e);
      }

      this.moveDown();
   }

   protected int pullNextEvent() {
      try {
         switch (this.parser.next()) {
            case 0:
            case 2:
               return 1;
            case 1:
            case 3:
               return 2;
            case 4:
               return 3;
            case 9:
               return 4;
            default:
               return 0;
         }
      } catch (Exception e) {
         throw new StreamException(e);
      }
   }

   protected String pullElementName() {
      return this.parser.getName();
   }

   protected String pullText() {
      return this.parser.getText();
   }

   public String getAttribute(String name) {
      return this.parser.getAttributeValue(null, this.encodeAttribute(name));
   }

   public String getAttribute(int index) {
      return this.parser.getAttributeValue(index);
   }

   public int getAttributeCount() {
      return this.parser.getAttributeCount();
   }

   public String getAttributeName(int index) {
      return this.decodeAttribute(this.parser.getAttributeName(index));
   }

   public void appendErrors(ErrorWriter errorWriter) {
      errorWriter.add("line number", String.valueOf(this.parser.getLineNumber()));
   }

   public void close() {
      try {
         this.reader.close();
      } catch (IOException var2) {
         throw new StreamException(var2);
      }
   }
}
