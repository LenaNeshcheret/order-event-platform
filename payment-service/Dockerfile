FROM eclipse-temurin:21-jre

WORKDIR /app
COPY payment-service/target/payment-service-*.jar app.jar

ENTRYPOINT ["java", "-jar", "/app/app.jar"]
