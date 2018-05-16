#!/usr/bin/env bash
set -xe

py.test skein/ -vv

flake8 skein/
