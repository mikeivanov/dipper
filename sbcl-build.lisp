(in-package :cl)
(asdf:load-system :dipper)

(defun exec ()
  (dipper::exec)
  (sb-ext:exit))

(sb-ext:save-lisp-and-die "dipper" :toplevel #'exec :executable t)
