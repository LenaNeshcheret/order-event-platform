FROM eclipse-temurin:21-jre

WORKDIR /app
COPY order-service/target/order-service-*.jar app.jar

EXPOSE 8080

ENTRYPOINT ["java", "-jar", "/app/app.jar"]
