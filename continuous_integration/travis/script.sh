#!/bin/env bash
set -xe

py.test skein/

flake8 skein/
