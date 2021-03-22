FROM openjdk:8-jre
ADD target/proc-0.1-jar-with-dependencies.jar app.jar
ENTRYPOINT ["java","-cp","/app.jar","by.nn.proc.driver.JoltTransform"]