package org.infinispan.rest.server.operations.mediatypes.printers;

public class Escaper {

   private Escaper() {

   }

   static String escapeHtml(String html) {
      return escapeXml(html);
   }

   static String escapeXml(String xml) {
      StringBuilder sb = new StringBuilder();
      for (char c : xml.toCharArray()) {
         switch (c) {
            case '&':
               sb.append("&amp;");
               break;
            case '>':
               sb.append("&gt;");
               break;
            case '<':
               sb.append("&lt;");
               break;
            case '\"':
               sb.append("&quot;");
               break;
            case '\'':
               sb.append("&apos;");
               break;
            default:
               sb.append(c);
               break;
         }
      }
      return sb.toString();
   }

   static String escapeJson(String json) {
      return json.replaceAll("\"", "\\\\\"");
   }

}
