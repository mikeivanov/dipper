#!/bin/bash
set -e
quicklisp=${QUICKLISP:-~/quicklisp}
mkdir -p dist

# prime the quicklisp cache and compile all the
# dependencies as a separate step
sbcl --eval '(require "dipper")' --eval '(quit)'

# actually build the thing
buildapp --output dist/dipper \
         --asdf-tree $quicklisp/dists/quicklisp/installed \
         --eval '(push #p"./" asdf:*central-registry*)' \
         --load-system dipper \
         --eval '(defun main (&args) (dipper::exec))' \
         --entry main

