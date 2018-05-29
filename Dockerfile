FROM openjdk:10-slim

ADD build/distributions/titanccp-configuration.tar /

EXPOSE 80

CMD /titanccp-configuration/bin/titanccp-configuration
