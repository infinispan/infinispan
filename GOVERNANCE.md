# Project Governance

## Project Overview
This project is maintained by a distributed team of maintainers working together to build and evolve Infinispan. 
We operate on consensus-based decision-making with clear processes for contribution and growth.

## Team Structure

### Core Maintainers
**Lead Maintainer**: [Tristan Tarrant] (@tristantarrant)
- **Responsibilities**: Final decision tiebreaker, release coordination, community leadership
- **Focus Area**: [Overall architecture, project direction]
- **Contact**: tristan@infinispan.org

**Community Maintainer**: [Katia Aresti] (@karesti)
- **Responsibilities**: Community engagement, contributor onboarding
- **Focus Area**: [Developer experience]
- **Contact**: katia@infinispan.org

### Maintainer Responsibilities
All maintainers share:
- Code review duties
- Issue triage and community support
- Release testing and validation
- Project roadmap planning
- New contributor mentoring

## Decision-Making Process

### Consensus Decisions (All maintainers must agree)
- Major architectural changes
- Breaking API changes
- Adding new maintainers
- Changing governance structure
- License changes
- Major dependency updates

### Majority Decisions (3+ maintainers agree)
- New feature additions
- Release timing and content
- Contribution guideline changes
- Infrastructure changes
- Community policy updates

### Individual Authority
Any maintainer can:
- Merge bug fixes and minor improvements
- Respond to issues and questions
- Create documentation updates
- Perform routine maintenance tasks

### Decision Timeline
- **Standard Decisions**: 1 week discussion period
- **Major Changes**: 2 week discussion + RFC if complex
- **Emergency Fixes**: 24-48 hours with post-hoc review

## Communication Channels

### Internal Team
- **Weekly Sync**: Mondays 1500 UTC (30 minutes)
- **Chat**: #infinispan channel on Zulip

### Community
- **Issues**: Bug reports, feature requests, general questions
- **Discussions**: RFC discussions, project feedback, community chat
- **PR Reviews**: Code contribution discussions
- **Email**: maintainers@infinispan.org for private matters

## Contribution Process

### New Contributors
1. **First Contribution**: Single maintainer approval for small fixes
2. **Regular Contributors**: After 3+ quality contributions, eligible for expanded review privileges
3. **Contributor Recognition**: Public acknowledgment and contributor badge

### Becoming a Maintainer
Requirements:
- 6+ months of consistent, quality contributions
- Deep understanding of project architecture and goals
- Positive community interactions and mentoring
- Availability for ongoing responsibilities (4+ hours/week)
- Unanimous approval from existing maintainers

Process:
1. **Nomination**: Any maintainer can nominate a contributor
2. **Discussion**: 2-week private maintainer discussion
3. **Vote**: Unanimous approval required
4. **Invitation**: Private invitation with role explanation
5. **Onboarding**: 1-month mentorship period

## Meeting Structure

### Weekly Maintainer Sync
- Current sprint progress
- Issue triage and prioritization
- Blocker identification and resolution
- Quick community updates

### Monthly Planning
- Roadmap review and updates
- Resource allocation
- Community health metrics
- Process improvements

### Quarterly Reviews
- Project goals assessment
- Governance effectiveness review
- Team health and satisfaction
- Strategic planning

## Conflict Resolution

### Minor Disagreements
1. **Discussion**: Extended conversation to understand viewpoints
2. **Research**: Gather additional data or community input
3. **Compromise**: Find mutually acceptable solution

### Major Conflicts
1. **Mediation**: Lead maintainer facilitates discussion
2. **Cooling Off**: 24-48 hour break if emotions run high
3. **Community Input**: Seek feedback from trusted contributors
4. **Vote**: Formal majority vote if consensus impossible
5. **Appeal**: Option to revisit decision after 3 months

## Release Process

### Release Types
- **Patch**: Bug fixes, minor improvements (any maintainer)
- **Minor**: New features, non-breaking changes (2+ maintainer approval)
- **Major**: Breaking changes, major features (all maintainer approval)

### Release Schedule
- **Patch Releases**: As needed
- **Minor Releases**: Monthly or when feature-complete
- **Major Releases**: Quarterly or when major milestones reached

### Release Checklist
1. Feature freeze and testing period
2. Documentation updates
3. Migration guides for breaking changes
4. Community announcement
5. Post-release monitoring

## Code Quality Standards
- **Test Coverage**: Minimum 80% for new code
- **Code Review**: 2 maintainer approvals for significant changes
- **Documentation**: All public APIs must be documented
- **Security**: Security review for authentication/authorization changes

## Code of Conduct
This project follows the project Code of Conduct. All maintainers are responsible for enforcement.

### Enforcement Process
1. **Warning**: Private message for minor violations
2. **Temporary Ban**: 1-7 days for repeated violations
3. **Permanent Ban**: For severe violations or repeated offenses
4. **Appeal Process**: Contact maintainers@infinispan.org

## Emergency Procedures

### Security Issues
1. **Immediate Response**: Within 2 hours during business hours
2. **Assessment**: Lead + technical maintainer evaluate severity
3. **Fix Development**: Immediate patch development if critical
4. **Disclosure**: Follow responsible disclosure timeline
5. **Post-Mortem**: Review and improve security processes

### Critical Bugs
1. **Immediate Triage**: Within 4 hours
2. **Hotfix Development**: Rush critical fixes to production
3. **Communication**: Transparent updates to community
4. **Post-Release**: Monitor for additional issues

## Contact Information
- **General Inquiries**: maintainers@infinispan.org
- **Security Issues**: security@infinispan.org
- **Code of Conduct**: conduct@infinispan.org
- **Private Matters**: tristan@infinispan.org

---
*This governance structure is designed for our current team size and will be reviewed quarterly for effectiveness and necessary adjustments.*
