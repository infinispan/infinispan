# {{jreleaserCreationStamp}}
{{#brewRequireRelative}}
require_relative "{{.}}"
{{/brewRequireRelative}}

class {{brewFormulaName}} < Formula
  desc "{{projectDescription}}"
  homepage "{{projectLinkHomepage}}"
  license "{{projectLicense}}"

  {{brewMultiPlatform}}
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
    # Each platform ZIP contains a single root directory
    # (infinispan-cli-<version>-<platform>/) wrapping the binary and completions.
    # In a multi-platform formula each platform's root directory has a different
    # name (the platform suffix differs), so it cannot be referenced by a single
    # JReleaser token — that would bake in only the first artifact's name and
    # break on every other platform. Resolve it at install time with a glob.
    root = Dir["infinispan-cli-*"].find { |d| File.directory?(d) } || "."
    cd root do
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
