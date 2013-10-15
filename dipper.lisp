(in-package :cl-user)

(defpackage :dipper
  (:use :cl :iterate :alexandria :unix-options
        :dipper.util :dipper.dbi :dipper.uri)
  (:import-from :dipper-asd :*dipper-version-string*)
  (:import-from :metabang-bind :bind))

(in-package :dipper)

(define-condition dipper-error (error)
  ((message :initarg :message :reader dipper-error-message)))

(defparameter +options+
  `(((#\d "database") "DATABASE" "Database connection")
    ((#\t "table") "TABLE" "Table name")
    ((#\c "columns") "COL1,COL2,...,COLN" "A comma-separated column list")
    ((#\i "incremental") "COLUMN" "Do an incremental update using this column")
    ((#\v "last-value") "VALUE" "The last seen value in the incremental column")
    ((#\l "limit") "N" "Limit the results to only N first rows.")
    ((#\u "username") "USERNAME" "Log in to DATABASE as USERNAME")
    ((#\p "password") "PASSWORD" "Log in to DATABASE using PASSWORD")
    ((#\o "output") "PATH" "Store results to PATH. Default: stdout")
    ((#\r "receipt") "PATH" "Read and write receipt file at PATH.")))

(defun parse-options (argv)
  (with-cli-options (argv)
      (&parameters database table columns incremental last-value limit
                   username password output receipt)
    (unless database
      (error "Database URI is a required parameter."))
    (unless table
      (error "Table name is a required parameter."))
    (when (and last-value (not incremental))
      (error "Incremental column is required when last value is specified."))
    (let ((uri (or (parse-database-uri database)
                   (error "'~A' is not a valid database URI." database)))
          (limit (when limit
                   (or (parse-integer (or limit "0") :junk-allowed t)
                       (error "'~A' is not a valid limit value" limit))))
          (columns (or columns "*"))
          (output (when output (parse-namestring output)))
          (receipt (when receipt (parse-namestring receipt))))
      (when username
        (setf (database-uri-username uri) username))
      (when password
        (setf (database-uri-password uri) password))
      (list :uri uri
            :table table
            :columns columns
            :incremental incremental
            :last-value last-value
            :limit limit
            :output-path output
            :receipt-path receipt))))

(defun dump-table (conn table data-stream
                   &key (columns "*") limit incremental last-value receipt)
  (let* ((limit-spec (if limit (format nil "LIMIT ~D" limit) ""))
         (last-value (or last-value (getf receipt :last-value)))
         (where-spec (if (and incremental last-value)
                         (format nil "WHERE ~A > ?" incremental)
                         ""))
         (parameters (if last-value (list last-value) nil))
         (sql-string (format nil "SELECT ~A FROM ~A ~A ~A"
                             columns table where-spec limit-spec))
         (resultset  (apply #'query (append (list conn sql-string)
                                            parameters)))
         (metadata   (metadata resultset))
         (incr-symbol (when incremental (string-to-keyword incremental)))
         (incr-index (when incremental
                       (iter (for col in metadata by #'cddr)
                             (for i from 0)
                             (finding i such-that (eql col incr-symbol)))))
         (last-value (iter (for row = (next-row resultset))
                           (while row)
                           (format data-stream "~{~A~^~T~}~%" row)
                           (when incremental
                             (maximize (elt row incr-index))))))
    (list :table table
          :columns columns
          :incremental incremental
          :last-value last-value)))

(defun read-receipt (path)
  (with-open-file (in path :direction :input :if-does-not-exist nil)
    (when in
      (yason:parse in
                   :object-as :plist
                   :object-key-fn #'string-to-keyword))))

(defun write-receipt (path receipt)
  (with-open-file (out path
                       :direction :output
                       :if-does-not-exist :create
                       :if-exists :rename)
    (yason:encode-plist receipt out)))

(defun main (&rest argv)
  (bind (((:plist uri table columns limit incremental
                  last-value output-path receipt-path) (parse-options argv))
         (receipt (when receipt-path (read-receipt receipt-path))))
    (with-connection (conn uri)
      (with-open-stream (out (if output-path
                                 (open output-path
                                       :direction :output
                                       :if-exists :overwrite
                                       :if-does-not-exist :create)
                                 *standard-output*))
        (let ((new-receipt (dump-table conn table out
                                       :columns columns
                                       :limit limit
                                       :incremental incremental
                                       :last-value last-value
                                       :receipt receipt)))
          (when receipt-path
            (write-receipt receipt-path new-receipt)))))))

(defun usage ()
  (print-usage-summary "Parameters:~%~@{~A~%~}~%" +options+))

(defun exec ()
  (handler-case
      (apply #'main (cli-options))
    (error (e)
      (format *error-output* "Dipper: ~A~%~%" e)
      (usage))))
