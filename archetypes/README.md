# The Infinispan Archetypes project

This project provides a set of quick-start archetypes which can be used to generate skeleton projects using [Infinispan](https://infinispan.org).

# Archetype Usage
To utilise the archetypes utilise the following command:

```
mvn archetype:generate \
    -DarchetypeGroupId=org.infinispan.archetypes \
    -DarchetypeArtifactId=<archetype-name> \
    -DarchetypeVersion=<infinispan-version> \
    -DarchetypeRepository=https://repository.jboss.org/nexus/content/groups/public
```

Where `<archetype-name>` can be one of the following:
  - `embedded` Creates a sample Embedded Infinispan application
  - `store` Creates a skeleton implementation of a custom Infinispan store
