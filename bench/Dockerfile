FROM eclipse-temurin:25-jre
COPY target/crossover.jar /app.jar
EXPOSE 8080
# G1 pinned explicitly: under small CPU limits the JVM would otherwise pick SerialGC for
# fine-grained services and G1 for the monolith, confounding the comparison.
ENTRYPOINT ["java", "-XX:+UseG1GC", "-XX:MaxRAMPercentage=50", "-jar", "/app.jar"]
