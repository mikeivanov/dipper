(in-package :cl-user)

(defpackage :dipper.util
  (:use :cl :iterate :alexandria)
  (:import-from :cl-ppcre :scan-to-strings)
  (:import-from :metabang-bind :bind)
  (:export :let-or
           :string-to-keyword
           :symbol-to-keyword
           :alist-get :alist-get-str
           :with-re-match
           :with-accessors-in))

(in-package :dipper.util)

(defun alist-get (key alist &key (test #'eql))
  (cdr (assoc key alist :test test)))

(defun alist-get-str (key alist)
  (cdr (assoc key alist :test #'equal)))

(defun string-to-keyword (string)
  (make-keyword (string-upcase string)))

(defun symbol-to-keyword (symbol)
  (string-to-keyword (symbol-name symbol)))

(defun with-re-match* (rx string callback)
  (when-let ((match (nth-value 1 (scan-to-strings rx string))))
    (apply callback (coerce match 'list))))

(defmacro with-re-match ((vars rx string) &body body)
  `(with-re-match* ,rx ,string (lambda (,@vars) ,@body)))

(defmacro with-accessors-in ((prefix slots object) &body body)
  `(bind (((:structure ,prefix ,@slots) ,object))
     ,@body))
