# Project Context: Infinispan

Infinispan is a distributed in-memory key/value data store and cache, written in Java.

## Shared guidelines
- Use the project's established patterns and terminology. When in doubt, follow the conventions of the module you are working in.
- Prefer clarity over cleverness. This is a long-lived open-source project with many contributors.
- Be aware of backward compatibility implications — Infinispan supports rolling upgrades and client/server version mismatches.

## Coding instructions
When planning and writing code, read and follow the instructions in AI-CODE.md.

## Testing instructions
When writing or modifying tests, read and follow the instructions in AI-TEST.md.

## Configuration change instructions
When adding or modifying configuration attributes, read and follow the instructions in AI-CONFIG.md.

## Documentation instructions
When writing or editing documentation, read and follow the instructions in documentation/AI-CODE.md.

## Issue creation instructions
When creating GitHub issues, read and follow the instructions in AI-ISSUES.md.

## Pull request review instructions
When reviewing PRs, read and follow the instructions in REVIEW.md.

## Maintaining AI context files
When making changes that alter the project structure, module organization, build process, or architectural patterns, check whether the relevant AI context files need updating: AI.md, AI-CODE.md, AI-TEST.md, AI-CONFIG.md, AI-ISSUES.md, documentation/AI-CODE.md, and any module-level AI-CODE.md files. The architecture documentation in `documentation/src/main/asciidoc/titles/architecture/` should also be checked when internal architecture changes. Examples of changes that warrant updates: renaming or moving modules, adding new modules, changing build commands, introducing new frameworks or test patterns, modifying configuration architecture, or updating development conventions.
