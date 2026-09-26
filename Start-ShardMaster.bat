@echo off
title ShardMaster - Unified Interactive Control Center (50,000,000 Rows)
chcp 65001 >nul
"%~dp0shardmaster.exe"
if %ERRORLEVEL% NEQ 0 pause
