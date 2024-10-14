@echo off
REM Use this script to run your program LOCALLY.
REM Note: Changing this script WILL NOT affect how CodeCrafters runs your program.

setlocal

REM Exit early if any commands fail
set "ERRORLEVEL=0"

REM Ensure compile steps are run within the repository directory
cd /d "%~dp0" || exit /b 1

REM Edit this to change how your program compiles locally
call mvn -B package -Ddir=/tmp/codecrafters-build-kafka-java > CON 2>&1
if %ERRORLEVEL% neq 0 exit /b %ERRORLEVEL%

REM Edit this to change how your program runs locally
java -jar /tmp/codecrafters-build-kafka-java/codecrafters-kafka.jar %*

