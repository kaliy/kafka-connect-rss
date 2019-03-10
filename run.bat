@echo off
setLocal EnableDelayedExpansion
set CLASSPATH="
for /R C:\Projects\kafka-connect-rss-atom\target\kafka-connect-rss-0.1.0-SNAPSHOT-package\share\java\kafka-connect-rss %%a in (*.jar) do (
  set CLASSPATH=!CLASSPATH!;%%a
)
set CLASSPATH=!CLASSPATH!"
echo %CLASSPATH%

C:\tools\kafka_2.12-2.1.1\bin\windows\connect-standalone.bat config/worker.properties config/RssSourceConnectorExample.properties

