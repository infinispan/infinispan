def pomFile = new File(basedir, "project/test/pom.xml")
assert pomFile.exists() : "pom.xml not found in generated project"

def pomContent = pomFile.text

// These Maven/parent properties must be resolved during archetype packaging (resource filtering).
// If any appear literally in the generated pom.xml, the filtering in archetypes/pom.xml is broken.
def unresolvedProperties = [
    '${project.version}',
    '${version.maven.compiler}',
    '${maven.compiler.source}',
    '${maven.compiler.target}'
]

for (prop in unresolvedProperties) {
    assert !pomContent.contains(prop) : "Generated pom.xml contains unresolved property: ${prop}"
}

true
