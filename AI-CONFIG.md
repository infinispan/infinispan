# Configuration Change Instructions

Adding or modifying a configuration attribute in Infinispan is a multi-file process. Missing a step will cause parsing failures, schema validation errors, or silent data loss on serialization.

## Architecture Overview

Configuration follows an immutable pattern with builders:

```
XSD Schema  →  Parser  →  ConfigurationBuilder (mutable)  →  Configuration (immutable)
                                                           ←  ConfigurationSerializer (write back)
```

Key packages:
- `org.infinispan.configuration.cache` — cache-level configuration classes and builders
- `org.infinispan.configuration.global` — global/container-level configuration
- `org.infinispan.configuration.parsing` — XML/JSON/YAML parsers, `Attribute` and `Element` enums
- `org.infinispan.configuration.serializing` — serializers that write configuration back to XML
- `core/src/main/resources/schema/` — XSD schema files

## Checklist: Adding a New Attribute

### 1. Add the Attribute Enum Value

**File:** `core/src/main/java/org/infinispan/configuration/parsing/Attribute.java`

Add the enum value in **alphabetical order**. The constructor auto-converts `MY_ATTRIBUTE` to the XML name `my-attribute`:

```java
MY_ATTRIBUTE,
```

### 2. Add the Element Enum Value (if adding a new sub-element)

**File:** `core/src/main/java/org/infinispan/configuration/parsing/Element.java`

Same pattern as Attribute — add in alphabetical order.

### 3. Define the AttributeDefinition in the Configuration Class

**File:** e.g., `core/src/main/java/org/infinispan/configuration/cache/IndexingConfiguration.java`

```java
public static final AttributeDefinition<MyType> MY_ATTRIBUTE =
    AttributeDefinition.builder(Attribute.MY_ATTRIBUTE, defaultValue)
        .immutable()  // omit if the attribute can change at runtime without restart
        .build();
```

Add a getter method:

```java
public MyType myAttribute() {
    return attributes.attribute(MY_ATTRIBUTE).get();
}
```

`AttributeDefinition` options:
- `.immutable()` — prevents runtime changes (requires cache restart)
- `.since(major, minor)` — schema version when the attribute was introduced
- `.copier(Copier)` — for collection-typed attributes
- `.initializer(Supplier)` — default value factory
- `.serializer(AttributeSerializer)` — custom serialization

### 4. Add the Builder Method

**File:** e.g., `core/src/main/java/org/infinispan/configuration/cache/IndexingConfigurationBuilder.java`

```java
public IndexingConfigurationBuilder myAttribute(MyType value) {
    attributes.attribute(MY_ATTRIBUTE).set(value);
    return this;
}
```

### 5. Add Parser Logic

**File:** `core/src/main/java/org/infinispan/configuration/parsing/CacheParser.java`

Add a `case` in the appropriate parse method's switch statement:

```java
case MY_ATTRIBUTE:
    builder.myAttribute(MyType.valueOf(value));
    break;
```

For enums, prefer a static validation method like `MyType.requireValid(value, CONFIG)`.

### 6. Add Serializer Logic

**File:** `core/src/main/java/org/infinispan/configuration/serializing/CoreConfigurationSerializer.java`

Add a write call in the appropriate method:

```java
attributes.write(writer, MyConfiguration.MY_ATTRIBUTE, Attribute.MY_ATTRIBUTE);
```

### 7. Update the XSD Schema

**File:** `core/src/main/resources/schema/infinispan-config-{version}.xsd`

Add the attribute to the appropriate complex type:

```xml
<xs:attribute name="my-attribute" type="xs:string" default="default-value"/>
```

For enum types, define a simple type:

```xml
<xs:simpleType name="my-type">
    <xs:restriction base="xs:token">
        <xs:enumeration value="option-a"/>
        <xs:enumeration value="option-b"/>
    </xs:restriction>
</xs:simpleType>
```

### 8. Create Enum Type (if needed)

If the attribute uses an enum, create the class:

```java
public enum MyType {
    OPTION_A,
    OPTION_B;

    public static MyType requireValid(String value, Log logger) {
        // validation logic
    }
}
```

If the enum is transmitted between nodes (e.g., part of cache mode or replication), annotate with `@Proto` and `@ProtoTypeId`.

## Server-Specific Configuration

Server configuration extends core with its own classes in `server/runtime/src/main/java/org/infinispan/server/configuration/`. The same pattern applies:
- `ServerConfigurationParser` for parsing
- Server-specific `Configuration` / `ConfigurationBuilder` classes
- Server schema in `server/runtime/src/main/resources/schema/`

## Common Mistakes

- **Forgetting the serializer** — the attribute parses correctly but is lost when configuration is saved/exported
- **Forgetting the XSD update** — schema validation fails for users with strict XML validation
- **Wrong alphabetical order in Attribute enum** — not a runtime error, but convention violation
- **Missing `immutable()` on attributes that should not change at runtime** — leads to undefined behavior if users modify them via JMX or REST
- **Not testing round-trip** — verify that parsing and re-serializing the configuration produces the same result
