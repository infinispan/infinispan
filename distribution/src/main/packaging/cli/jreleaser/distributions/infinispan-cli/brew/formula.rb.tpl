# {{jreleaserCreationStamp}}
{{#brewRequireRelative}}
require_relative "{{.}}"
{{/brewRequireRelative}}

class {{brewFormulaName}} < Formula
  desc "{{projectDescription}}"
  homepage "{{projectLinkHomepage}}"
  url "{{distributionUrl}}"{{#brewDownloadStrategy}}, :using => {{.}}{{/brewDownloadStrategy}}
  sha256 "{{distributionChecksumSha256}}"
  license "{{projectLicense}}"

  {{#brewHasLivecheck}}
  livecheck do
    {{#brewLivecheck}}
    {{.}}
    {{/brewLivecheck}}
  end
  {{/brewHasLivecheck}}
  {{#brewDependencies}}
  depends_on {{.}}
  {{/brewDependencies}}

  def install
    # The ZIP contains a single root directory
    # (infinispan-cli-<version>-<platform>/) wrapping the binary and completions.
    cd "{{distributionArtifactRootEntryName}}" do
      bin.install "{{distributionExecutableUnix}}"
      bash_completion.install "completions/infinispan-cli_complete.bash" => "infinispan-cli"
      bash_completion.install "completions/infinispan-cli_dynamic_complete.bash" => "infinispan-cli-dynamic"
      zsh_completion.install "completions/infinispan-cli_complete.zsh" => "_infinispan-cli"
      zsh_completion.install "completions/infinispan-cli_dynamic_complete.zsh" => "_infinispan-cli-dynamic"
      fish_completion.install "completions/infinispan-cli.fish"
      fish_completion.install "completions/infinispan-cli_dynamic.fish" => "infinispan-cli-dynamic.fish"
    end
  end

  test do
    output = shell_output("#{bin}/{{distributionExecutableName}} --version")
    assert_match "{{projectVersion}}", output
  end
end
