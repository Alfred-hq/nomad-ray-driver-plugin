package templates

const ActorHealthStatusScript = `
#!/bin/bash

# Check if the actor ID was provided
if [ -z "$1" ]
then
  echo "Please provide an ACTOR_ID."
  exit 1
fi

# Assign the actor ID to a variable
ACTOR_ID=$1

# Use ray list actors and grep to find the specific actor ID and extract the STATE
state=$(ray list actors | grep $ACTOR_ID | awk '{print $4}')

# Check if a state was found
if [ -z "$state" ]
then
  printf "%s" "state not found"
else
  printf "%s" "$state"
fi
`
