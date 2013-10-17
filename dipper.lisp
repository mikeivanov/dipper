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
  `((("database") "DATABASE" "Database connection")
    (("table") "TABLE" "Table name")
    (("columns") "COL1,COL2,...,COLN" "A comma-separated column list")
    (("incremental") "COLUMN" "Do an incremental update using this column")
    (("last-value") "VALUE" "The last seen value in the incremental column")
    (("limit") "N" "Limit the results to only N first rows.")
    (("username") "USERNAME" "Log in to DATABASE as USERNAME")
    (("password") "PASSWORD" "Log in to DATABASE using PASSWORD")
    (("output") "PATH" "Store results to PATH. Default: stdout")
    (("receipt") "PATH" "Read and write receipt file at PATH.")
    (("config") "PATH" "Read config from PATH.")))

(defun parse-options (argv)
  (let ((options (make-hash-table))
        (names (iter (for ((name)) in +options+)
                     (collect name))))
    (flet ((next-option (name value)
             (unless value
               (error "Option '~A' must have a value" name))
             (let ((key (string-to-keyword name)))
               (setf (gethash key options) value)))
           (unexpected (arg)
             (error "Unexpected argument '~A'." arg)))
      (map-parsed-options argv () names #'next-option #'unexpected))
    options))

(defun read-ini-file (path)
  (let ((ini (py-configparser:make-config)))
    (py-configparser:read-files ini (list path))
    ini))

(defgeneric getconf (thing key &optional default))

(defmethod getconf ((config py-configparser:config) key &optional default)
  (let ((name (symbol-name key)))
    (if (py-configparser:has-option-p config "dipper" name)
        (py-configparser:get-option config "dipper" name)
        default)))

(defmethod getconf ((hash hash-table) key &optional default)
  (gethash key hash default))

(defmethod getconf ((configs list) key &optional default)
  (if-let ((config (first configs)))
    (or (getconf config key)
        (getconf (rest configs) key default))
    default))

(defmethod getconf ((env (eql :env)) key &optional default)
  (let ((var (format nil "DIPPER_~A"
                     (string-upcase (symbol-name key)))))
    (or (environment-variable var)
         default)))

(defmethod getconf ((thing t) key &optional default)
  (declare (ignore thing)
           (ignore key))
  default)

(defun prepare-parameters (config)
  (macrolet ((param (var &key then else)
               `(let ((,var (getconf config ,(string-to-keyword (symbol-name var)))))
                  (or (when ,var ,(or then var))
                      ,(or else nil)))))
    (let* ((database (param database
                            :else (error "Database URI is not specified.")))
           (table (param table
                         :else (error "Table name is not specified.")))
           (limit (param limit
                         :then (or (parse-integer limit :junk-allowed t)
                                   (error "'~A' is not a valid limit value" limit))))
           (columns (param columns :else "*"))
           (output-path (param output
                               :then (parse-namestring output)))
           (incremental (param incremental
                               :then (string-downcase incremental)))
           (last-value (param last-value
                              :then (unless incremental
                                      (error "Incremental is not specified"))))
           (receipt-path (param receipt :then (parse-namestring receipt)))
           (username (param username))
           (password (param password))
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
            :output-path output-path
            :receipt-path receipt-path))))

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

(defun make-config (argv)
  (let* ((options (parse-options argv))
         (config-path (getconf (list options :env) :config))
         (ini (when config-path
                (read-ini-file config-path))))
    (list options ini :env)))

(defun main (&rest argv)
  (bind ((config (make-config argv))
         ((:plist uri table columns limit incremental
                  last-value output-path
                  receipt-path) (prepare-parameters config))
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
