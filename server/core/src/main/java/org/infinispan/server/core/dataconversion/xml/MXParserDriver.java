package org.infinispan.server.core.dataconversion.xml;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;

import org.infinispan.commons.configuration.io.xml.MXParser;

import com.thoughtworks.xstream.core.util.XmlHeaderAwareReader;
import com.thoughtworks.xstream.io.AbstractDriver;
import com.thoughtworks.xstream.io.HierarchicalStreamReader;
import com.thoughtworks.xstream.io.HierarchicalStreamWriter;
import com.thoughtworks.xstream.io.StreamException;
import com.thoughtworks.xstream.io.xml.CompactWriter;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.0
 **/
public class MXParserDriver extends AbstractDriver {

   public MXParserDriver() {}

   @Override
   public HierarchicalStreamReader createReader(Reader reader) {
      return new MXParserReader(reader, new MXParser(), getNameCoder());
   }

   @Override
   public HierarchicalStreamReader createReader(InputStream in) {
      try {
         return createReader(new XmlHeaderAwareReader(in));
      } catch (IOException e) {
         throw new StreamException(e);
      }
   }

   @Override
   public HierarchicalStreamWriter createWriter(Writer writer) {
      return new CompactWriter(writer, getNameCoder());
   }

   @Override
   public HierarchicalStreamWriter createWriter(OutputStream out) {
      return createWriter(new OutputStreamWriter(out));
   }
}
