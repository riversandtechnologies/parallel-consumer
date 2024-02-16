#!/bin/bash

RELEASE_TAG=snapshot

git tag -d $RELEASE_TAG
git push origin :refs/tags/$RELEASE_TAG

git tag $RELEASE_TAG
git push origin $RELEASE_TAG