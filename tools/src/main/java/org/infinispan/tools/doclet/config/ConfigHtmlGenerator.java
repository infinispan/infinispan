package org.infinispan.tools.doclet.config;

import java.util.List;
import java.util.Map;

import org.infinispan.Version;
import org.infinispan.config.AbstractConfigurationBean;
import org.infinispan.config.InfinispanConfiguration;
import org.infinispan.tools.schema.AbstractTreeWalker;
import org.infinispan.tools.schema.TreeNode;
import org.infinispan.tools.schema.XSOMSchemaTreeWalker;
import org.infinispan.util.ClassFinder;
import org.infinispan.util.TypedProperties;

import com.sun.javadoc.ClassDoc;
import com.sun.javadoc.FieldDoc;
import com.sun.javadoc.Tag;

/**
 * Infinispan configuration reference guide generator
 *
 * @author Vladimir Blagojevic
 * @since 4.0
 */

@SuppressWarnings("restriction")
public class ConfigHtmlGenerator extends AbstractConfigHtmlGenerator {

    private static final String CONFIG_PROPERTY_REF = "configPropertyRef";           

   String classpath;
   
   public ConfigHtmlGenerator(String encoding, String title, String bottom, String footer,
                              String header, String metaDescription, List<String> metaKeywords, String classpath) {
      super(encoding, title, bottom, footer, header, metaDescription, metaKeywords);
      this.classpath = classpath;

   }

   protected List<Class<?>> getConfigBeans() throws Exception {
      List<Class<?>> list = ClassFinder.isAssignableFrom(ClassFinder.infinispanClasses(classpath),
                                          AbstractConfigurationBean.class);
      
      list.add(TypedProperties.class);
      list.add(InfinispanConfiguration.class);
      return list;
   }
   
   protected String getSchemaFile() {
       return "schema/infinispan-config-" + Version.getMajorVersion() + ".xsd";
   }
   
   protected String getTitle() {
       return "<h2>Infinispan configuration options "  + Version.getMajorVersion() + " </h2><br/>";
   }
   
   protected String getRootElementName() {
       return "infinispan";
   }
   
   protected void preXMLTableOfContentsCreate(XSOMSchemaTreeWalker sw, XMLTreeOutputWalker tw) {
       
       TreeNode root = sw.getRoot();
       TreeNode node = tw.findNode(root, "namedCache", "infinispan");
       node.detach();

       PruneTreeWalker ptw = new PruneTreeWalker("property");
       ptw.postOrderTraverse(root);              
   }
   
   protected boolean preVisitNode(TreeNode n) {
       return n.getName().equals("properties");           
   }
   
   protected boolean postVisitNode(TreeNode n) {
       //generate table for properties as well                 
       if (n.hasChild("properties")) {
          generatePropertiesTableRows(getStringBuilder(), n);
       }
       return super.postVisitNode(n);
   }
   
   private void generatePropertiesTableRows(StringBuilder sb, TreeNode n) { 
       FieldDoc fieldDoc = fieldDocWithTag(n.getBeanClass(), CONFIG_PROPERTY_REF);              
       if (fieldDoc != null) {
          sb.append("<table class=\"bodyTable\"> ");
          sb.append("<tr class=\"a\"><th>Property</th><th>Description</th></tr>\n");
          Tag[] tags = fieldDoc.tags(CONFIG_PROPERTY_REF);
          for (Tag t : tags) {
             Map<String, String> m = parseTag(t.text().trim());
             sb.append("<tr class=\"b\">");
             sb.append("<td>").append(m.get(CONFIG_REF_NAME_ATT)).append("</td>\n");
             sb.append("<td>").append(m.get(CONFIG_REF_DESC_ATT)).append("</td>\n");
             sb.append("</tr>\n");
          }
          sb.append("</table>\n");
       }
    }
   
   private FieldDoc fieldDocWithTag(Class<?> c, String tagName) {
       while (true) {
           ClassDoc classDoc = rootDoc.classNamed(c.getName());
           for (FieldDoc fd : classDoc.fields()) {
               if (fd.tags(tagName).length > 0) {                  
                   return fd;
               }
           }

           if (c.getSuperclass() != null) {
               c = c.getSuperclass();
               continue;
           }
           return null;
       }
   }

   private static class PruneTreeWalker extends AbstractTreeWalker {

      private String pruneNodeName = "";

      private PruneTreeWalker(String pruneNodeName) {
         super();
         this.pruneNodeName = pruneNodeName;
      }

      public void visitNode(TreeNode treeNode) {
         if (treeNode.getName().equals(pruneNodeName)) {
            treeNode.detach();
         }
      }
   }
}
