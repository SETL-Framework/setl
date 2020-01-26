#!/bin/bash

export AWS_ACCESS_KEY_ID="fakeAccess"
export AWS_SECRET_ACCESS_KEY="fakeSecret"
export AWS_REGION="eu-west-1"

mvn -B -ntp clean:clean scoverage:report
