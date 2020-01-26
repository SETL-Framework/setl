#!/bin/bash

echo ${MVN_SETTINGS} | base64 -d > /root/.m2/settings.xml
echo ${MVN_SECURITY} | base64 -d > /root/.m2/settings-security.xml

mvn clean deploy scala:doc -ntp -B -DskipTests -Psnapshot -P${SCALA_PROFILE}
