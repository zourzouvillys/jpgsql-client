#!/bin/sh

set -e

if [ ! -f ./.gradle/gradlew ]; then
  echo "auto-generating gradle wrapper"
  gradle wrapper --gradle-version 4.2.1
fi

exec ./.gradle/gradlew "$@" 


