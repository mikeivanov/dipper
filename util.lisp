(in-package :cl-user)

(defpackage :dipper.util
  (:use :cl :iterate :alexandria)
  (:import-from :cl-ppcre :scan-to-strings)
  (:import-from :metabang-bind :bind)
  (:export :string-to-keyword
           :symbol-to-keyword
           :alist-get :alist-get-str
           :with-re-match
           :with-accessors-in
           :terminate
           :getenv))

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

(defun terminate (status)
  #+sbcl       (sb-ext:quit :unix-status status) ; SBCL
  #+ccl        (ccl:quit status)                 ; Clozure CL
  #+clisp      (ext:quit status)                 ; GNU CLISP
  #+cmu        (unix:unix-exit status)           ; CMUCL
  #+abcl       (ext:quit :status status)         ; Armed Bear CL
  #+allegro    (excl:exit status :quiet t)       ; Allegro CL
  #+lispworks  (lispworks:quit :status status)   ; LispWorks
  #+ecl        (ext:quit status)                 ; ECL
  (cl-user::quit))

(defun getenv (name &optional default)
    #+CMU
    (let ((x (assoc name ext:*environment-list*
                    :test #'string=)))
      (if x (cdr x) default))
    #-CMU
    (or
     #+Allegro (sys:getenv name)
     #+CLISP (ext:getenv name)
     #+ECL (si:getenv name)
     #+SBCL (sb-unix::posix-getenv name)
     #+CCL (ccl:getenv name)
     #+LISPWORKS (lispworks:environment-variable name)
     default))
