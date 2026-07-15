@echo off
setlocal
set "CONFIG=%~1"
if "%CONFIG%"=="" goto :usage
if not exist "%CONFIG%" goto :usage
where java >nul 2>nul
if errorlevel 1 (
  echo Java is required but was not found on PATH. 1>&2
  exit /b 1
)
set "PROJECT_DIR=%~dp0.."
set "JAR=%PROJECT_DIR%\target\kafka-producer-test-tool.jar"
if not exist "%JAR%" for %%F in ("%PROJECT_DIR%\target\kafka-producer-test-tool-*.jar") do set "JAR=%%~fF"
if not exist "%JAR%" (
  echo Application JAR not found. Build it with mvn package. 1>&2
  exit /b 1
)
java -jar "%JAR%" "--spring.config.additional-location=file:%CONFIG%" --kafka-producer-test.mode=VALIDATE
exit /b %errorlevel%
:usage
echo Usage: %~nx0 ^<readable-run-config.yaml^> 1>&2
exit /b 2
