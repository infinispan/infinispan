package org.infinispan.tools.doclet.config;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.infinispan.Version;
import org.infinispan.config.AbstractConfigurationBean;
import org.infinispan.config.InfinispanConfiguration;
import org.infinispan.tools.schema.AbstractTreeWalker;
import org.infinispan.tools.schema.TreeNode;
import org.infinispan.tools.schema.XSOMSchemaTreeWalker;
import org.infinispan.util.ClassFinder;
import org.infinispan.util.TypedProperties;

/**
 * Infinispan configuration reference guide generator
 *
 * @author Vladimir Blagojevic
 * @since 4.0
 */
public class ConfigHtmlGenerator extends AbstractConfigHtmlGenerator {         

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
      Field field = findField(n.getBeanClass(), "properties");
      if (field != null) {
         Map<String, String> description = findDescription(field);
         if (!description.isEmpty()) {
            sb.append("<table class=\"bodyTable\"> ");
            sb.append("<tr class=\"a\"><th>Property</th><th>Description</th></tr>\n");
            for (Entry<String, String> e : description.entrySet()) {
               sb.append("<tr class=\"b\">");
               sb.append("<td>").append(e.getKey()).append("</td>\n");
               sb.append("<td>").append(e.getValue()).append("</td>\n");
               sb.append("</tr>\n");
            }
            sb.append("</table>\n");
         }
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
