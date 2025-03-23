FROM openjdk:19-buster

ENV MB_PLUGINS_DIR=/home/plugins/

ADD https://downloads.metabase.com/v0.53.7/metabase.jar /home
ADD ./target/metabase-driver-0.1.0-SNAPSHOT.jar /home/plugins/metabase-driver-0.1.0-SNAPSHOT.jar

RUN chmod 744 /home/plugins/metabase-driver-0.1.0-SNAPSHOT.jar

CMD ["java", "-jar", "/home/metabase.jar"]