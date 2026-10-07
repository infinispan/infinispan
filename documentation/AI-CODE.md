# Documentation Module Instructions

## Content Organization

Infinispan documentation follows Red Hat Modular Documentation with a three-layer hierarchy:

```
Title (guide/book)  →  titles/{guide}/{guide}.asciidoc
  └── Story (assembly)  →  stories/assembly_*.adoc
      └── Topic (reusable content)  →  topics/{con,proc,ref}_*.adoc
          └── Code/Config examples  →  topics/{code_examples,cmd_examples,rest_examples,xml,json,yaml,...}
```

### Titles
- Located in `src/main/asciidoc/titles/{guide}/`
- Each title has a main `.asciidoc` file and a `stories.adoc` that includes assemblies
- Sets document attributes and `:context:`

### Stories (Assemblies)
- Located in `src/main/asciidoc/stories/`
- Named `assembly_*.adoc`
- Assemble multiple topics into a user journey
- Set and restore `:context:` for anchor scoping
- Include topics with `include::{topics}/con_*.adoc[leveloffset=+N]`

### Topics
- Located in `src/main/asciidoc/topics/`
- Three types with strict naming prefixes:
  - **`con_*`** — Concept: explains what something is and why it matters
  - **`proc_*`** — Procedure: step-by-step instructions (uses `.Procedure` header with numbered steps)
  - **`ref_*`** — Reference: tables, API details, configuration options
- Each topic must have an ID: `[id='descriptive-name_{context}']`
- Topics are designed for reuse across multiple guides

### Code and Configuration Examples
- Java code examples: `topics/code_examples/*.java`
- CLI command examples: `topics/cmd_examples/`
- REST request examples: `topics/rest_examples/`
- Configuration files by format: `topics/xml/`, `topics/json/`, `topics/yaml/`, `topics/properties/`
- Include in topics with: `include::code_examples/MyClass.java[]`

## Writing Style

### Voice and Tense
- **Active voice**, present tense, second person ("you")
- Never use first person ("we", "I")
- No contractions ("do not", not "don't")
- American English spelling

### Formatting
- One sentence per line (hard wrap at sentence boundaries, not at column width)
- Use `{brandname}` attribute, never hardcode "Infinispan" in content
- File paths, class names, XML attributes: backticks (`` ` ``)
- GUI elements: bold (`*Add*`)
- First occurrence of a term: italics (`_High availability_`)
- Numbers below 10: spell out ("four"); 10 and above: numerals ("12")
- Avoid Latin abbreviations (use "for example" not "e.g.", "that is" not "i.e.")
- Never use "simply" unless it genuinely clarifies

### Section IDs and Cross-References
- Section IDs use underscores: `[id='cache_interface_{context}']`
- Reusable topics must include `{context}` in their ID
- Internal links: `link:#anchor_name[Link Text]`

### Admonitions
```asciidoc
[NOTE]
====
Note content.
====
```
Use `NOTE`, `TIP`, `WARNING`, `IMPORTANT` as appropriate.

### Code Blocks
```asciidoc
[source,java,options="nowrap",subs=attributes+]
.MyClass.java
----
include::code_examples/MyClass.java[]
----
```

### Diagrams
- Prefer Asciidoctor Diagram blocks over pre-rendered images; they are rendered at build time and scale without loss of quality.
- UML-style diagrams (class, sequence, flowcharts): `[plantuml, target=diagram-name, format=svg, align=center]` block with PlantUML source (`@startuml`/`@enduml`). The theme comes from the `:plantuml-theme:` attribute; do not hardcode colors.
- Diagram content is passed to PlantUML verbatim unless you opt in to substitutions. If a diagram references attributes such as `{brandname}`, add `subs="attributes+"` to the block header (for example, `[plantuml, target=diagram-name, format=svg, align=center, subs="attributes+"]`) or Asciidoctor leaves the attribute reference unresolved in the rendered image.
- Syntax or tree-like diagrams: `syntrax` blocks (jsyntrax), e.g., `[syntrax, diagram-name, format=svg, align=center]`. See `topics/ref_query_ickle_syntax.adoc` for examples.
- Only when a diagram cannot be expressed with Asciidoctor Diagrams at all: save it as SVG under `topics/images/`. Never use PNG/JPG for diagrams.

### Screenshots and Raster Images
- Use PNG or JPG only for user interface screenshots (minimum 660px wide, maximum 300KB).
- Location: `topics/images/`
- All images must have alt text for accessibility
- Syntax: `image::filename.png[Alt text description]`

## Conditional Content

Use `ifdef` / `endif` for community vs enterprise content:
```asciidoc
ifdef::community[]
Community-only content here.
endif::community[]
```

## Terminology

Follow the terminology defined in `topics/contributing/terminology.adoc`. Key terms:
- **Cache Manager** — two words, capitalized (use `CacheManager` only for the Java interface)
- **Off-heap** — always hyphenated when used as an adjective
- **Query** — use "query" not "search" for looking up information
- **Reindex** — one word, no hyphen
- **Add/Remove** — for container membership; **Create/Delete** — for building/destroying objects; **Clear** — delete all elements

## Building Documentation
- Build HTML: `mvn install -pl documentation`
- Build PDF: `mvn install -pl documentation -Ppdf`
- Output: `documentation/target/generated/{version}/html/`

## When Creating New Documentation
1. Determine if the content is a concept, procedure, or reference — use the correct `con_`/`proc_`/`ref_` prefix
2. Place the topic file in `topics/`
3. Create or update an assembly in `stories/` to include the new topic
4. Update the relevant `stories.adoc` in the title directory if adding a new assembly
5. Always set `[id='descriptive-name_{context}']` at the top of the topic
6. Include code examples as separate files, not inline
