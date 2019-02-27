#!/usr/bin/env bash
mvn clean package -Pprovided -DskipTests -Dapp.environment=dev -T 4

