@echo off
title ShardMaster - Unified Interactive Control Center
chcp 65001 >nul
"%~dp0shardmaster.exe"
if %ERRORLEVEL% NEQ 0 pause
