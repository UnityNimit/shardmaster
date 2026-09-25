@echo off
title ShardMaster v2.0 - Unified Interactive Control Center (50,000,000 Rows)
chcp 65001 >nul
"%~dp0shardmaster.exe"
if %ERRORLEVEL% NEQ 0 pause
