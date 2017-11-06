#!/bin/bash

set -e

cd $(dirname "$0")

if [ ! -f ./gradle/gradlew ]; then
  gradle wrapper
fi

exec ./gradle/gradlew "$@"

