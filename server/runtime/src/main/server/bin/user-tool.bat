@echo off

echo "This tool is deprecated and will be removed in the future. Please use \"cli.bat user create\" instead"

set "DIRNAME=%~dp0%"

setlocal EnableDelayedExpansion
call "!DIRNAME!cli.bat" user create %*
setlocal DisableDelayedExpansion
