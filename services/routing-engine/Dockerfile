FROM maven:3.9-eclipse-temurin-17 AS builder
WORKDIR /app
COPY pom.xml .
RUN mvn dependency:go-offline -B
COPY src ./src
RUN mvn package -DskipTests -B

FROM eclipse-temurin:17-jre-jammy
WORKDIR /app
COPY --from=builder /app/target/routing-engine-1.0.0.jar app.jar
EXPOSE 8081
ENTRYPOINT ["java", "-Xmx128m", "-jar", "app.jar"]
