#!/bin/bash

echo ${MVN_SETTINGS} > /root/.m2/settings.xml
echo ${MVN_SECURITY} > /root/.m2/settings-security.xml

mvn deploy scala:doc -ntp -B -DskipTests -Psnapshot -Pwith-source -P${SCALA_PROFILE}
