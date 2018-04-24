package org.infinispan.doclets.html;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.List;

/**
 * Generates HTML documents
 *
 * @author Manik Surtani
 * @since 4.0
 */
public abstract class HtmlGenerator {
   String encoding, title, bottom, footer, header, metaDescription;
   List<String> metaKeywords;

   public HtmlGenerator(String encoding, String title, String bottom, String footer, String header, String metaDescription, List<String> metaKeywords) {
      this.encoding = encoding;
      this.title = title;
      this.footer = footer;
      this.header = header;
      this.bottom = bottom;
      this.metaDescription = metaDescription;
      this.metaKeywords = metaKeywords;
   }

   public void generateHtml(String fileName) throws IOException {
      generateHtml(fileName, "stylesheet.css");
   }

   public void generateHtml(String fileName, String styleSheetName) throws IOException {
      FileOutputStream fos = new FileOutputStream(fileName);
      OutputStreamWriter osw = isValid(encoding) ? new OutputStreamWriter(fos, encoding) : new OutputStreamWriter(fos);
      PrintWriter writer = new PrintWriter(osw);
      try {
         writer.println("<!DOCTYPE html PUBLIC \"-//W3C//DTD XHTML 1.0 Transitional//EN\"\n" +
               "\t\"http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd\">");
         writer.println("<HTML xmlns=\"http://www.w3.org/1999/xhtml\" xml:lang=\"en\" lang=\"en\">");
         writer.println("<HEAD>");
         if (isValid(metaDescription))
            writer.println("<META NAME=\"description\" content=\"" + metaDescription + "\" />");
         if (metaKeywords != null && !metaKeywords.isEmpty()) {
            StringBuilder sb = new StringBuilder();
            sb.append("<META NAME=\"keywords\" content=\"");
            for (String keyword : metaKeywords) sb.append(keyword).append(", ");
            sb.append("\" />");
         }
         writer.println("<TITLE>");
         writer.println(title);
         writer.println("</TITLE>");
         writer.println("<LINK REL=\"stylesheet\" HREF=\"" + styleSheetName + "\" TYPE=\"text/css\"/>");

         writer.println("</HEAD>");
         writer.println("<BODY>");

         if (isValid(header)) {
            writer.println(header);
            writer.println("<HR />");
         }

         writer.println(generateContents());

         if (isValid(bottom)) {
            writer.println("<HR />");
            writer.println(bottom);
         }

         if (isValid(footer)) writer.println(footer);


         writer.println("</BODY>");
         writer.println("</HTML>");
      } finally {
         writer.close();
         osw.close();
         fos.close();
      }
   }

   protected abstract String generateContents();

   protected boolean isValid(String s) {
      return s != null && s.trim().length() != 0;
   }
}
