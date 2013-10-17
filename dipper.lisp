(in-package :cl-user)

(defpackage :dipper
  (:use :cl :iterate :alexandria :unix-options
        :dipper.util :dipper.dbi :dipper.uri)
  (:import-from :dipper-asd :*dipper-version-string*)
  (:import-from :osicat :environment-variable)
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
    ((#\r "receipt") "PATH" "Read and write receipt file at PATH.")
    ((#\g "config") "PATH" "Read config from PATH.")))

(defun make-config ()
  (make-hash-table))

(defun read-config (path)
  (let ((ini (py-configparser:make-config)))
    (py-configparser:read-files ini (list path))
    (let ((items (py-configparser:items ini "dipper")))
      (alist-hash-table (iter (for (k . v) in items)
                              (collect (cons (string-to-keyword k) v)))))))

(defun set-config (config key value)
  (setf (gethash config key) value))

(defun get-config (config key &optional default)
  (gethash key config default))

(defun parse-options (argv)
  (with-cli-options (argv)
      (&parameters database table columns incremental last-value limit
                   username password output receipt config)
    ;; TODO: refactor it
    (let* ((config (or config
                       (environment-variable "DIPPER_CONFIG")))
           (cfg (if config
                   (read-config (parse-namestring config))
                   (make-config)))
           (database (or database
                         (get-config cfg :database)
                         (environment-variable "DIPPER_DATABASE")
                         (error "Database URI is not specified.")))
           (table (or table
                      (get-config cfg :table)
                      (environment-variable "DIPPER_TABLE")
                      (error "Table name is not specified.")))
           (last-value (or last-value
                           (get-config cfg :last-value)
                           (environment-variable "DIPPER_LAST_VALUE")))
           (incremental (or incremental
                            (get-config cfg :incremental)
                            (environment-variable "DIPPER_INCREMENTAL")
                            (when last-value
                              (error "Incremental column should be specified if the last value is given."))))
           (limit (let ((limit (or limit
                                   (get-config cfg :limit)
                                   (environment-variable "DIPPER_LIMIT"))))
                    (when limit
                      (or (parse-integer limit :junk-allowed t)
                          (error "'~A' is not a valid limit value" limit)))))
           (columns (or columns
                        (get-config cfg :columns)
                        (environment-variable "DIPPER_COLUMNS")
                        "*"))
           (output (let ((output (or output
                                     (get-config cfg :output)
                                     (environment-variable "DIPPER_OUTPUT"))))
                     (when output (parse-namestring output))))
           (incremental (let ((incremental (or incremental
                                               (get-config cfg :incremental)
                                               (environment-variable "DIPPER_INCREMENTAL"))))
                          (when incremental (string-downcase incremental))))
           (receipt (let ((receipt (or receipt
                                       (get-config cfg :receipt)
                                       (environment-variable "DIPPER_RECEIPT"))))
                      (when receipt (parse-namestring receipt))))
           (uri (or (parse-database-uri database
                                        :override (list :username username
                                                        :password password))
                    (error "'~A' is not a valid database URI." database))))
      (list :uri uri
            :table table
            :columns columns
            :incremental incremental
            :last-value last-value
            :limit limit
            :output-path output
            :receipt-path receipt))))

(defun dump-resultset (stream resultset incremental comparator)
  (iter (for row = (next-row resultset))
        (while row)
        (format stream "~{~A~^~T~}~%" row)
        (when incremental
          (for val = (elt row incremental))
          (reducing val by (lambda (a b) (if (funcall comparator a b) b a))))))

(defun variant< (a b)
  (cond ((numberp a) (< a b))
        (t (string< a b))))

(defparameter *type-comparators* (list :var-string #'string<
                                       :text       #'string<
                                       :null       #'variant<))

(defun get-type-comparator (type)
  (getf *type-comparators* type #'<))

(defun dump-table (conn table data-stream
                   &key (columns "*") limit incremental last-value)
  (bind ((limit-spec (if limit (format nil "LIMIT ~D" limit) ""))
         (where-spec (if (and incremental last-value)
                         (format nil "WHERE ~A > ?" incremental)
                         ""))
         (parameters (if last-value (list last-value) nil))
         (sql-string (format nil "SELECT ~A FROM ~A ~A ~A"
                             columns table where-spec limit-spec))
         (resultset  (query conn sql-string parameters))
         (metadata   (metadata resultset))
         ((idx . ct) (or (when incremental
                           (iter (for (col . type) in metadata)
                                 (for i from 0)
                                 (finding (cons i type)
                                          such-that (equal col incremental))))
                         (cons nil nil)))
         (comparator (get-type-comparator ct))
         (new-value  (dump-resultset data-stream
                                     resultset
                                     idx
                                     comparator)))
    ;; TODO: should I care about receipts here?
    (list :table table
          :columns columns
          :incremental incremental
          :last-value (or new-value last-value))))

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
                                       :if-exists :supersede
                                       :if-does-not-exist :create)
                                 (make-synonym-stream '*standard-output*)))
        (let* ((last-value (or last-value (getf receipt :last-value)))
               (new-receipt (dump-table conn table out
                                        :columns columns
                                        :limit limit
                                        :incremental incremental
                                        :last-value last-value)))
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
