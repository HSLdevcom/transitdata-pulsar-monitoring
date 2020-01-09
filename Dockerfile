FROM openjdk:8-jre-slim
#Install curl for health check
RUN apt-get update && apt-get install -y --no-install-recommends curl
ADD target/transitdata-pulsar-monitoring.jar /usr/app/transitdata-pulsar-monitoring.jar
ENTRYPOINT ["java", "-jar", "/usr/app/transitdata-pulsar-monitoring.jar"]
