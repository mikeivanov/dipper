(in-package :cl-user)
(defpackage :dipper.dbi
  (:use :cl :iterate :dipper.util :dipper.uri :alexandria)
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

(defun driver-type (conn)
  (let* ((class (class-of conn))
         (symbol (class-name class))
         (name (symbol-name symbol)))
    (with-re-match ((type-name) "^<DBD-([^-]+)-.*>$" name)
      (string-to-keyword type-name))))

(defgeneric query- (type conn sql &optional parameters))

(defparameter *mysql-string-to-type-map* (make-hash-table))

(defmethod query- ((type (eql :mysql)) conn sql &optional parameters)
  (let* ((cl-mysql:*type-map* *mysql-string-to-type-map*)
         (statement (dbi:prepare conn sql)))
    (apply #'dbi:execute (cons statement parameters))))

(defmethod query- ((type t) conn sql &optional parameters)
  (let ((statement (dbi:prepare conn sql)))
    (apply #'dbi:execute (cons statement parameters))))

(defun query (conn sql &optional parameters)
  (query- (driver-type conn) conn sql parameters))

(defgeneric metadata- (type query))

(defparameter *mysql-to-dbi-type-map*
  (plist-hash-table (list
                      :DECIMAL    :decimal
                      :TINY       :int
                      :SHORT      :int
                      :LONG       :long
                      :FLOAT      :float
                      :DOUBLE     :double
                      :NULL       :string
                      :TIMESTAMP  :datetime
                      :LONGLONG   :long
                      :INT24      :int
                      :DATE       :date
                      :TIME       :time
                      :DATETIME   :datetime
                      :YEAR       :int
                      :NEWDATE    :datetime
                      :NEWDECIMAL :decimal)))

(defmethod metadata- ((type (eql :mysql)) query)
  (let* ((result (slot-value query 'dbd.mysql::%result))
         (fields (car (cl-mysql::result-set-fields result))))
    (iter (for (name . type) in fields)
          (collect (cons (string-downcase name)
                         (gethash (car type)
                                  *mysql-string-to-type-map*
                                  :string))))))

(defmethod metadata- ((type (eql :sqlite3)) query)
  (let* ((statement (dbi.driver:query-prepared query))
         (handle (sqlite::handle statement))
         (ncols (sqlite-ffi:sqlite3-column-count handle)))
    (iter (for i from 0 below ncols)
          (collect (cons (string-downcase (sqlite-ffi:sqlite3-column-name handle i))
                         (sqlite-ffi:sqlite3-column-type handle i))))))

(defun metadata (query)
  (let ((conn (dbi.driver::query-connection query)))
    (metadata- (driver-type conn) query)))

(defun next-row (query)
  (let* ((cl-mysql:*type-map* *mysql-string-to-type-map*))
    (let ((row (dbi:fetch query)))
      (iter (for lst on row by #'cddr)
            (collect (cadr lst))))))
