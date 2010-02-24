package org.infinispan.tools.doclet.config;

import com.sun.javadoc.ClassDoc;
import com.sun.javadoc.FieldDoc;
import com.sun.javadoc.RootDoc;
import com.sun.javadoc.Tag;
import com.sun.xml.xsom.XSAttributeDecl;
import com.sun.xml.xsom.XSFacet;
import com.sun.xml.xsom.XSRestrictionSimpleType;
import com.sun.xml.xsom.XSSchemaSet;
import com.sun.xml.xsom.parser.XSOMParser;
import org.infinispan.Version;
import org.infinispan.config.AbstractConfigurationBean;
import org.infinispan.config.InfinispanConfiguration;
import org.infinispan.tools.doclet.html.HtmlGenerator;
import org.infinispan.tools.schema.AbstractTreeWalker;
import org.infinispan.tools.schema.TreeNode;
import org.infinispan.tools.schema.XSOMSchemaTreeWalker;
import org.infinispan.util.ClassFinder;
import org.infinispan.util.FileLookup;
import org.infinispan.util.TypedProperties;

import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

/**
 * Infinispan configuration reference guide generator
 *
 * @author Vladimir Blagojevic
 * @since 4.0
 */
@SuppressWarnings("restriction")
public class ConfigHtmlGenerator extends HtmlGenerator {

   private static final String CONFIG_REF = "configRef";
   private static final String CONFIG_PROPERTY_REF = "configPropertyRef";

   private static final int LEVEL_MULT = 3;
   private static final boolean DEBUG = Boolean.parseBoolean(System.getProperty("infinispan.tools.configdoc.debug", "false"));

   String classpath;
   RootDoc rootDoc;

   public ConfigHtmlGenerator(String encoding, String title, String bottom, String footer,
                              String header, String metaDescription, List<String> metaKeywords, String classpath) {
      super(encoding, title, bottom, footer, header, metaDescription, metaKeywords);
      this.classpath = classpath;

   }

   public RootDoc getRootDoc() {
      return rootDoc;
   }

   public void setRootDoc(RootDoc rootDoc) {
      this.rootDoc = rootDoc;
   }

   protected List<Class<?>> getConfigBeans() throws Exception {
      return ClassFinder.isAssignableFrom(ClassFinder.infinispanClasses(classpath),
                                          AbstractConfigurationBean.class);
   }

   protected String generateContents() {

      StringBuilder sb = new StringBuilder();
      // index of components
      sb.append("<h2>Infinispan configuration options "  + Version.getMajorVersion() + " </h2><br/>");
      sb.append("<UL>");

      List<Class<?>> configBeans;
      try {
         configBeans = getConfigBeans();
         configBeans.add(TypedProperties.class);
         configBeans.add(InfinispanConfiguration.class);

         XMLTreeOutputWalker tw = new XMLTreeOutputWalker(sb);
         FileLookup fl = new FileLookup();
         InputStream file = fl.lookupFile("schema/infinispan-config-" + Version.getMajorVersion() + ".xsd");
         XSOMParser reader = new XSOMParser();
         reader.parse(file);
         XSSchemaSet xss = reader.getResult();
         XSOMSchemaTreeWalker w = new XSOMSchemaTreeWalker(xss.getSchema(1), "infinispan");
         TreeNode root = w.getRoot();

         associateBeansWithTreeNodes(configBeans, root);

         TreeNode node = tw.findNode(root, "namedCache", "infinispan");
         node.detach();

         PruneTreeWalker ptw = new PruneTreeWalker("property");
         ptw.postOrderTraverse(root);

         sb.append("<div class=\"" + "source" + "\"><pre>");
         // print xml tree into StringBuilder
         tw.preOrderTraverse(root);
         sb.append("</pre></div>");


         for (TreeNode n : root) {

            //do not generate element for properties element 
            if (n.getName().equals("properties"))
               continue;

            debug("element = " + n.getName(), 0);

            // Name, description, parent and child elements node
            generateHeaderForConfigurationElement(sb, tw, n);

            //now attributes
            if (!n.getAttributes().isEmpty()) {
               generateAttributeTableRows(sb, n);
            }

            //and properties....                 
            if (n.hasChild("properties")) {
               generatePropertiesTableRows(sb, n);
            }
         }

      } catch (Exception e) {
         System.out.println("Exception while generating configuration reference " + e);
         System.out.println("Classpath is  " + classpath);
         e.printStackTrace();
      }
      return sb.toString();
   }

   private void associateBeansWithTreeNodes(List<Class<?>> configBeans, TreeNode root) {
      for (TreeNode n : root) {
         if (n.getBeanClass() == null) {
            for (Class<?> clazz : configBeans) {
               ClassDoc classDoc = rootDoc.classNamed(clazz.getName());
               if (classDoc != null) {
                  List<Tag> list = Arrays.asList(classDoc.tags(CONFIG_REF));
                  for (Tag tag : list) {
                     String text = tag.text().trim();
                     Map<String, String> p = parseTag(text);
                     String thisNode = p.get("name");
                     String parentNode = p.get("parentName");
                     if (n.getName().equalsIgnoreCase(thisNode)) {
                        // parent specified
                        if (parentNode != null
                              && parentNode.equalsIgnoreCase(n.getParent().getName())) {
                           n.setBeanClass(clazz);
                        } else if (parentNode == null) {
                           n.setBeanClass(clazz);
                        }
                     }
                  }
               }
            }
         }
      }
   }

   private void generatePropertiesTableRows(StringBuilder sb, TreeNode n) {
      FieldDoc fieldDoc = fieldDocWithTag(n.getBeanClass(), CONFIG_PROPERTY_REF);
      if (fieldDoc != null) {
         sb.append("\n<table class=\"bodyTable\"> ");
         sb.append("<tr class=\"a\"><th>Property</th><th>Description</th></tr>\n");
         Tag[] tags = fieldDoc.tags(CONFIG_PROPERTY_REF);
         for (Tag t : tags) {
            Map<String, String> m = parseTag(t.text().trim());
            sb.append("<tr class=\"b\">");
            sb.append("<td>").append(m.get("name")).append("</td>\n");
            sb.append("<td>").append(m.get("desc")).append("</td>\n");
            sb.append("</tr>\n");
         }
         sb.append("</table></div>");
      }
   }

   private void generateAttributeTableRows(StringBuilder sb, TreeNode n) {

      sb.append("<table class=\"bodyTable\"> ");
      sb.append("<tr class=\"a\"><th>Attribute</th><th>Type</th><th>Default</th><th>Description</th></tr>\n");

      Class<?> bean = n.getBeanClass();
      Object object = null;
      try {
         Constructor<?>[] constructors = bean.getDeclaredConstructors();
         for (Constructor<?> c : constructors) {
            if (c.getParameterTypes().length == 0) {
               c.setAccessible(true);
               object = c.newInstance();
            }
         }
      } catch (Exception e) {
         System.out.println("Did not construct object " + bean);
      }

      Set<XSAttributeDecl> attributes = n.getAttributes();
      for (XSAttributeDecl a : attributes) {
         sb.append("<tr class=\"b\">");

         //name, type...
         sb.append("<td>").append("<code>" + a.getName() + "</code>").append("</td>\n");
         sb.append("<td>").append("<code>" + a.getType().getName() + "</code>");

         boolean isRestricted = false;
         XSRestrictionSimpleType restriction = a.getType().asRestriction();
         Collection<? extends XSFacet> declaredFacets = restriction.getDeclaredFacets();
         for (XSFacet facet : declaredFacets) {
            if (facet.getName().equalsIgnoreCase("enumeration")) {
               isRestricted = true;
               break;
            }
         }

         debug ("attribute = " + a.getName() + "(restricted = " + isRestricted + ")", 1);

         // restriction on type...
         if (isRestricted) {
            sb.append("* (");
            for (XSFacet facet : declaredFacets) {
               sb.append(facet.getValue().toString() + '|');
            }
            sb.deleteCharAt(sb.length() - 1);
            sb.append(")</td>\n");
         } else {
            sb.append("</td>\n");
         }

         // if default value specified in annotation use it
         if (a.getDefaultValue() != null) {
            debug("annotation-defined default = " + a.getDefaultValue(), 2);
            sb.append("<td>").append(a.getDefaultValue().toString()).append("</td>\n");
         }

         // otherwise reflect that field and read default value
         else {
            Field field = null;
            Object defaultValue = null;
            try {
               field = findFieldRecursively(bean, a.getName());
               defaultValue = fieldValue(field, object);
               if (defaultValue != null) {
                  sb.append("<td>").append(defaultValue.toString()).append("</td>\n");
                  debug("field-defined default = " + defaultValue, 2);
               } else {
                  debug("field-defined default is null!", 2);
                  sb.append("<td>").append("null").append("</td>\n");
               }
            } catch (Exception e) {
               debug("Caught exception, bean is " + bean.getName() + ", looking for field " + a.getName() + ", field " + field, 2);               
               e.printStackTrace();
               sb.append("<td>").append("N/A").append("</td>\n");
            }
         }

         // and finally description
         FieldDoc fieldDoc = findFieldDocRecursively(bean, a.getName(), CONFIG_REF);
         if (fieldDoc != null) {
            Tag[] tags = fieldDoc.tags(CONFIG_REF);
            Map<String, String> p = parseTag(tags[0].text().trim());
            sb.append("<td>").append(p.get("desc")).append("\n");
            String packageDir = fieldDoc.containingPackage().toString().replace(".", "/").concat("/");
            String htmlFile = fieldDoc.containingClass().typeName().concat(".html");
            String field = fieldDoc.name();
            sb.append(" (<a href=\"" +packageDir.concat(htmlFile).concat("#").concat(field) +"\">" + "Javadoc</a>)");
            sb.append("</td>\n");
            
         }
         sb.append("</tr>\n");
      }
      sb.append("</table></div>");
   }

   private void debug(String s, int level) {
      if (DEBUG) {
         StringBuilder sb = new StringBuilder();
         for (int i = 0; i < level * LEVEL_MULT; i++) sb.append(" ");
         sb.append("> ").append(s);
         System.out.println(sb.toString());
      }
   }

   public Map<String, String> parseTag(String tag) {

      //javadoc parser for our tags
      Map<String, String> p = new HashMap<String, String>();
      Scanner sc = new Scanner(tag);
      Scanner sc2 = null;
      sc.useDelimiter("\"\\s*,\\s*");
      try {
         while (sc.hasNext()) {
            String keyValue = sc.next();
            sc2 = new Scanner(keyValue);
            sc2.useDelimiter("=\\s*\"");
            String key = sc2.next();
            String value = sc2.next().replace("\"", "");
            p.put(key, value);
            sc2.close();
         }
         sc.close();
      } catch (Exception e) {
         System.out.println("Invalid tag " + tag + " skipping...");
      } finally {
         sc.close();
         sc2.close();
      }
      return p;
   }

   private void generateHeaderForConfigurationElement(StringBuilder sb, XMLTreeOutputWalker tw, TreeNode n) {
      sb.append("\n<a name=\"").append("ce_" + n.getParent().getName() + "_" + n.getName() + "\">" + "</a>");
      sb.append("<div class=\"section\"><h3><a name=\"" + n.getName() + "\"></a>" + n.getName() + "</h3>");
      sb.append("\n<p>");
      Class<?> beanClass = n.getBeanClass();
      //System.out.println("Generating " + n + " bean is " + beanClass);
      ClassDoc classDoc = rootDoc.classNamed(beanClass.getName());
      Tag[] tags = classDoc.tags(CONFIG_REF);
      for (Tag tag : tags) {
         String text = tag.text().trim();
         Map<String, String> m = parseTag(text);
         sb.append(m.get("desc"));
      }

      sb.append("<BR/><BR />");

      if (n.getParent().getParent() != null) {
         sb.append("The parent element is " + "<a href=\"").append("#ce_" + n.getParent().getParent().getName()
               + "_" + n.getParent().getName() + "\">" + "&lt;" + n.getParent().getName() + "&gt;" + "</a>.  ");
      }

      if (!n.getChildren().isEmpty()) {
         int childCount = n.getChildren().size();
         int count = 1;
         if (childCount == 1)
            sb.append("The only child element is ");
         else
            sb.append("Child elements are ");
         for (TreeNode tn : n.getChildren()) {
            sb.append("<a href=\"").append("#ce_" + tn.getParent().getName() + "_"
                  + tn.getName() + "\">" + "&lt;" + tn.getName() + "&gt;" + "</a>");
            if (count < childCount) {
               sb.append(", ");
            } else {
               sb.append(".");
            }
            count++;
         }
         sb.append("\n");
      }
      sb.append("</p>");
   }

   private Field findFieldRecursively(Class<?> c, String fieldName) {
      findFieldRecursively:
      while (true) {
         Field f = null;
         try {
            f = c.getDeclaredField(fieldName);
         } catch (NoSuchFieldException e) {
            ClassDoc classDoc = rootDoc.classNamed(c.getName());
            for (FieldDoc fd : classDoc.fields()) {
               for (Tag t : fd.tags(CONFIG_REF)) {
                  Map<String, String> m = parseTag(t.text().trim());
                  String field = m.get("name");
                  if (field != null && field.startsWith(fieldName)) {
                     fieldName = fd.name();
                     continue findFieldRecursively;
                  }
               }
            }
            if (!c.equals(Object.class))
               f = findFieldRecursively(c.getSuperclass(), fieldName);
         }
         return f;
      }
   }

   private FieldDoc findFieldDocRecursively(Class<?> c, String fieldName, String tagName) {
      while (true) {
         ClassDoc classDoc = rootDoc.classNamed(c.getName());
         for (FieldDoc fd : classDoc.fields()) {
            if (fd.name().equalsIgnoreCase(fieldName)) {
               return fd;
            }
            for (Tag t : fd.tags(tagName)) {
               Map<String, String> m = parseTag(t.text().trim());
               if (m.containsKey("name")) {
                  String value = m.get("name").trim();
                  if (fieldName.equalsIgnoreCase(value)) {
                     return fd;
                  }
               }
            }
         }
         if (c.getSuperclass() != null) {
            c = c.getSuperclass();
            continue;
         }
         return null;
      }
   }

   private FieldDoc fieldDocWithTag(Class<?> c, String tagName) {
      ClassDoc classDoc = rootDoc.classNamed(c.getName());
      for (FieldDoc fd : classDoc.fields()) {
         if (fd.tags(tagName).length > 0)
            return fd;

      }
      if (c.getSuperclass() != null)
         fieldDocWithTag(c.getSuperclass(), tagName);
      return null;
   }

   private static Object fieldValue(Field field, Object target) {
      if (!Modifier.isPublic(field.getModifiers())) {
         field.setAccessible(true);
      }
      try {
         return field.get(target);
      } catch (IllegalAccessException iae) {
         throw new IllegalArgumentException("Could not get field " + field, iae);
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
