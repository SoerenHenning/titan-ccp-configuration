FROM openjdk:10-slim

ADD build/distributions/titanccp-configuration.tar /

EXPOSE 80

CMD /titanccp-aggregation/bin/titanccp-configuration
