package org.infinispan.tools;

import java.util.Objects;

import org.w3c.dom.Node;

/**
 * Dependency from licenses.xml file.
 * <p>
 * It contains the group/artifact/version and the corresponding XML {@link Node}.
 * <p>
 * Note: the artifact is "empty" for NodeJS dependencies.
 *
 * @author Pedro Ruivo
 * @since 12.0
 */
public class Dependency {
   private final String group;
   private final String artifact;
   private final String version;
   private final Node node;

   Dependency(String group, String artifact, String version, Node node) {
      this.group = group;
      this.artifact = artifact;
      this.version = version;
      this.node = node;
   }

   public String getGroup() {
      return group;
   }

   public String getArtifact() {
      return artifact;
   }

   public String getVersion() {
      return version;
   }

   public Node getNode() {
      return node;
   }

   public String getId() {
      return String.format("%s:%s", group, artifact);
   }

   @Override
   public String toString() {
      return "Dependency{" +
            "group='" + group + '\'' +
            ", artifact='" + artifact + '\'' +
            ", version='" + version + '\'' +
            '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) {
         return true;
      }
      if (o == null || getClass() != o.getClass()) {
         return false;
      }
      Dependency that = (Dependency) o;
      return group.equals(that.group) &&
            artifact.equals(that.artifact) &&
            version.equals(that.version) &&
            node.equals(that.node);
   }

   @Override
   public int hashCode() {
      return Objects.hash(group, artifact, version, node);
   }
}
