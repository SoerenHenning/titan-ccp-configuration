FROM openjdk:11-slim

ADD build/distributions/titanccp-configuration.tar /

EXPOSE 80

CMD JAVA_OPTS="$JAVA_OPTS -Dorg.slf4j.simpleLogger.defaultLogLevel=$LOG_LEVEL" \
    /titanccp-configuration/bin/titanccp-configuration