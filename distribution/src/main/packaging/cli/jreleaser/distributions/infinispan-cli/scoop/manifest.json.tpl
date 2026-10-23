{
    "version": "{{projectVersion}}",
    "description": "{{projectDescription}}",
    "homepage": "{{projectLinkHomepage}}",
    "license": "{{projectLicense}}",
    "url": "{{distributionUrl}}",
    "hash": "sha256:{{distributionChecksumSha256}}",
    "extract_dir": "{{distributionArtifactRootEntryName}}",
    "bin": "{{distributionExecutableWindows}}",
    "checkver": {
        "url": "{{scoopCheckverUrl}}",
        "re": "releases/tag/([\\d.]+)"
    },
    "autoupdate": {
        "url": "{{scoopAutoupdateUrl}}",
        "extract_dir": "{{scoopAutoupdateExtractDir}}",
        "hash": {
            "url": "$url.sha256"
        }
    }
}