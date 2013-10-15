(in-package :cl-user)
(defpackage :dipper.dbi
  (:use :cl :iterate :dipper.util :dipper.uri)
  (:export :with-connection :query :next-row :metadata))

(in-package :dipper.dbi)

(defgeneric make-connection (driver uri))

(defmethod make-connection ((type (eql :mysql)) (uri database-uri))
  (with-accessors-in (database-uri- (host port username password path) uri)
    (let ((dbname (string-left-trim '(#\/) path)))
      (dbi:connect type
                   :host (or host "127.0.0.1")
                   :port port
                   :database-name dbname
                   :username username
                   :password password))))

(defmethod make-connection ((type (eql :sqlite3)) (uri database-uri))
  (with-accessors-in (database-uri- (subprotocol path) uri)
    (dbi:connect type :database-name (or path subprotocol))))

(defun connect (uri)
  (make-connection (string-to-keyword (database-uri-scheme uri)) uri))

(defmacro with-connection ((conn-var uri) &body body)
  `(let ((,conn-var (connect ,uri)))
     (unwind-protect
          (progn ,@body)
       (dbi:disconnect ,conn-var))))

(defun query (conn sql &rest parameters)
  (let ((statement (dbi:prepare conn sql)))
    (apply #'dbi:execute (cons statement parameters))))

(defun driver-type (instance)
  (let* ((conn (slot-value instance 'dbi.driver::connection))
         (class (class-of conn))
         (symbol (class-name class))
         (name (symbol-name symbol)))
    (with-re-match ((type-name) "^<DBD-([^-]+)-.*>$" name)
      (string-to-keyword type-name))))

(defgeneric get-metadata (type query))

(defmethod get-metadata ((type (eql :mysql)) query)
  (let* ((result (slot-value query 'dbd.mysql::%result))
         (fields (car (cl-mysql::result-set-fields result))))
    (iter (for (name . type) in fields)
          (appending (list (string-to-keyword name) type)))))

(defmethod get-metadata ((type (eql :sqlite3)) query)
  (let* ((statement (dbi.driver:query-prepared query))
         (handle (sqlite::handle statement))
         (ncols (sqlite-ffi:sqlite3-column-count handle)))
    (iter (for i from 0 below ncols)
          (appending (list (string-to-keyword (sqlite-ffi:sqlite3-column-name handle i))
                           (sqlite-ffi:sqlite3-column-type handle i))))))

(defun metadata (query)
  (get-metadata (driver-type query) query))

(defun next-row (query)
  (let ((row (dbi:fetch query)))
    (iter (for lst on row by #'cddr)
          (collect (cadr lst)))))
