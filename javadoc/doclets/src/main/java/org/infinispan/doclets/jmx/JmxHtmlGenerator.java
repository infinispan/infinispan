package org.infinispan.doclets.jmx;

import java.util.List;

import org.infinispan.doclets.html.HtmlGenerator;

public class JmxHtmlGenerator extends HtmlGenerator {
   List<MBeanComponent> components;

   public JmxHtmlGenerator(String encoding, String title, String bottom, String footer, String header, String metaDescription, List<String> metaKeywords, List<MBeanComponent> components) {
      super(encoding, title, bottom, footer, header, metaDescription, metaKeywords);
      this.components = components;
   }

   @Override
   protected String generateContents() {
      StringBuilder sb = new StringBuilder();
      // index of components
      sb.append("<h2>JMX Components available</h2><br />");
      sb.append("<UL>");
      for (MBeanComponent mbean : components) {
         sb.append("<LI><A HREF=\"#").append(mbean.name).append("\">").append(mbean.name).append("</A>");
         if (isValid(mbean.desc)) sb.append(" <I>(").append(mbean.desc).append(")</I>");
         sb.append("</LI>\n");
      }
      sb.append("</UL>");
      sb.append("<BR /><BR /><HR /><BR />");

      // a table for each component.
      for (MBeanComponent mbean : components) {
         sb.append("<A NAME=\"").append(mbean.name).append("\">\n");
         sb.append("<TABLE WIDTH=\"100%\" CELLSPACING=\"1\" CELLPADDING=\"0\" BORDER=\"1\">\n");
         sb.append("<TR CLASS=\"TableHeadingColor\"><TH ALIGN=\"LEFT\"><b>Component <tt>").append(mbean.name).append("</tt></b>  (Class <TT><A HREF=\"")
               .append(toURL(mbean.className)).append("\">").append(mbean.className).append("</A></TT>)");
         if (isValid(mbean.desc)) sb.append("<br /><I>").append(mbean.desc).append("</I>\n");
         sb.append("</TH></TR>\n");

         if (!mbean.attributes.isEmpty()) {
            // Attributes
            sb.append("<TR CLASS=\"TableSubHeadingColor\"><TH ALIGN=\"LEFT\"><strong><i>Attributes</i></strong></TH></TR>\n");
            sb.append("<TR BGCOLOR=\"white\" CLASS=\"TableRowColor\"><TD ALIGN=\"CENTER\"><TABLE WIDTH=\"100%\" cellspacing=\"1\" cellpadding=\"0\" border=\"0\">\n");
            sb.append("<TR CLASS=\"TableSubHeadingColor\"><TD ALIGN=\"LEFT\" VALIGN=\"TOP\"><strong>Name</strong></TD>\n");
            sb.append("<TD ALIGN=\"LEFT\" VALIGN=\"TOP\" WIDTH=\"40%\"><strong>Description</strong></TD>\n");
            sb.append("<TD ALIGN=\"LEFT\" VALIGN=\"TOP\"><strong>Type</strong></TD>\n");
            sb.append("<TD ALIGN=\"LEFT\" VALIGN=\"TOP\"><strong>Writable</strong></TD></TR>\n");
            for (MBeanAttribute attr : mbean.attributes) {
               sb.append("<TR BGCOLOR=\"white\" CLASS=\"TableRowColor\">");
               sb.append("<TD ALIGN=\"LEFT\" VALIGN=\"TOP\"><tt>").append(attr.name).append("</tt></TD>");
               sb.append("<TD ALIGN=\"LEFT\" VALIGN=\"TOP\">").append(attr.desc).append("</TD>");
               sb.append("<TD ALIGN=\"LEFT\" VALIGN=\"TOP\"><tt>").append(attr.type).append("</tt></TD>");
               sb.append("<TD ALIGN=\"LEFT\" VALIGN=\"TOP\">").append(attr.writable).append("</TD>");
               sb.append("</TR>");
            }
            sb.append("</TABLE></TD></TR>");
         }

         if (!mbean.operations.isEmpty()) {
            // Operations
            sb.append("<TR CLASS=\"TableSubHeadingColor\"><TH ALIGN=\"LEFT\"><strong><i>Operations</i></strong></TH></TR>\n");
            sb.append("<TR BGCOLOR=\"white\" CLASS=\"TableRowColor\"><TD ALIGN=\"CENTER\"><TABLE WIDTH=\"100%\" cellspacing=\"1\" cellpadding=\"0\" border=\"0\">\n");
            sb.append("<TR CLASS=\"TableSubHeadingColor\"><TD ALIGN=\"LEFT\" VALIGN=\"TOP\"><strong>Name</strong></TD>\n");
            sb.append("<TD ALIGN=\"LEFT\" WIDTH=\"50%\" VALIGN=\"TOP\"><strong>Description</strong></TD>\n");
            sb.append("<TD ALIGN=\"LEFT\" VALIGN=\"TOP\"><strong>Signature</strong></TD></TR>\n");
            for (MBeanOperation operation : mbean.operations) {
               sb.append("<TR BGCOLOR=\"white\" CLASS=\"TableRowColor\">");
               sb.append("<TD ALIGN=\"LEFT\" VALIGN=\"TOP\"><tt>").append(operation.name).append("</tt></TD>");
               sb.append("<TD ALIGN=\"LEFT\" VALIGN=\"TOP\">").append(operation.desc).append("</TD>");
               sb.append("<TD ALIGN=\"LEFT\" VALIGN=\"TOP\"><tt>").append(generateSignature(operation)).append("</tt></TD>");
               sb.append("</TR>");
            }
            sb.append("</TABLE></TD></TR>");
         }

         sb.append("</TABLE><BR /><BR />");
      }

      return sb.toString();
   }

   private String toURL(String fqcn) {
      return fqcn.replace(".", "/") + ".html";
   }

   private String generateSignature(MBeanOperation op) {
      // <retType> <name>(<args>)
      StringBuilder sb = new StringBuilder();
      if (isValid(op.returnType))
         sb.append(op.returnType);
      else
         sb.append("void");

      sb.append(" ").append(op.name);
      if (isValid(op.signature))
         sb.append("(").append(op.signature).append(")");
      else
         sb.append("()");
      return sb.toString();
   }
}
