@echo off

set "DIRNAME=%~dp0%"
pushd "%DIRNAME%.."
set "BASEDIR_RESOLVED=%CD%"
popd

git config get --local core.hooksPath
if ERRORLEVEL 1 (
   git config set --local core.hooksPath "%BASEDIR_RESOLVED%/.githooks
)
git config get --local commit.template
if ERRORLEVEL 1 (
   git config set --local commit.template "%BASEDIR_RESOLVED%/.gitmessage
)
