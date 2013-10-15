(in-package :cl-user)

(defpackage :dipper-asd
  (:use :cl :asdf))

(in-package :dipper-asd)

(defvar *dipper-version-string* "0.0.1"
  "Dipper's version number as a string.")

(export '*dipper-version-string*)

(defsystem :dipper
  :description "A data dumping tool"
  :serial t
  :version #.*dipper-version-string*
  :depends-on (:iterate :cl-ppcre :dbi :dbd-mysql :dbd-sqlite3
               :unix-options :alexandria :metabang-bind :yason)
  :components ((:file "util")
               (:file "uri")
               (:file "dbi")
               (:file "dipper")))

(defsystem :dipper-tests
  :description "A test suite for Dipper"
  :serial t
  :version #.*dipper-version-string*
  :depends-on (:dipper :iterate :fiveam :osicat :uuid)
  :components ((:file "dipper-tests")))
