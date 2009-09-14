@echo off
set jruby_path=%~dp0jruby\bin\jruby.bat
set rubyrep_path=%~dp0bin\rubyrep
%jruby_path% --server %rubyrep_path% %1 %2 %3 %4 %5 %6 %7 %8 %9
