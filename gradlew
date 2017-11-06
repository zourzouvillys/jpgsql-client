#!/bin/sh

set -e

if [ ! -f ./.gradle/gradlew ]; then
  echo "auto-generating gradle wrapper"
  gradle wrapper
fi

exec ./.gradle/gradlew "$@" 


