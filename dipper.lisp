(in-package :cl-user)

(defpackage :dipper
  (:use :cl :iterate :alexandria :unix-options
        :dipper.util :dipper.dbi :dipper.uri)
  (:import-from :dipper-asd :*dipper-version-string*)
  (:import-from :metabang-bind :bind))

(in-package :dipper)

(cl-interpol:enable-interpol-syntax)

(define-condition dipper-error (error)
  ((message :initarg :message :reader dipper-error-message)))

(defparameter +options+
  `((("help") nil "Help saves the world")
    (("database") "DATABASE" "Database connection")
    (("table") "TABLE" "Table name")
    (("columns") "COL1,COL2,...,COLN" "A comma-separated column list")
    (("incremental") "COLUMN" "Do an incremental update using this column")
    (("last-value") "VALUE" "The last seen value in the incremental column")
    (("limit") "N" "Limit the results to only N first rows.")
    (("username") "USERNAME" "Log in to DATABASE as USERNAME")
    (("password") "PASSWORD" "Log in to DATABASE using PASSWORD")
    (("output") "PATH" "Store results to PATH. Default: stdout")
    (("receipt") "PATH" "Read and write receipt file at PATH.")
    (("write-receipt") "PATH" "Override the receipt destination specified by --receipt.")
    (("config") "PATH" "Read config from PATH.")))

(defun parse-options (argv)
  (let ((options (make-hash-table))
        (optional ())
        (boolean ()))
    (iter (for ((name) opt) in +options+)
          (if opt
              (push name optional)
              (push name boolean)))
    (flet ((next-option (name value)
             (unless value
               (error "Option '~A' must have a value" name))
             (let ((key (string-to-keyword name)))
               (setf (gethash key options) value)))
           (unexpected (arg)
             (error "Unexpected argument '~A'." arg)))
      (map-parsed-options argv boolean optional #'next-option #'unexpected))
    options))

(defun read-ini-file (path)
  (when (probe-file path)
    (let ((ini (py-configparser:make-config)))
      (py-configparser:read-files ini (list path))
      ini)))

(defun write-ini-file (ini path)
  (with-open-file (out path
                       :direction :output
                       :if-exists :supersede
                       :if-does-not-exist :create)
    (py-configparser:write-stream ini out)))

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
    (getenv var default)))

(defmethod getconf ((thing t) key &optional default)
  (declare (ignore thing)
           (ignore key))
  default)

(defun prepare-parameters (config)
  (macrolet ((param (var &key then else)
               `(let* ((,var (getconf config ,(symbol-to-keyword var)))
                       (,var (if (equal ,var "DEFAULT") nil ,var)))
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
           (read-receipt-path (param receipt :then (parse-namestring receipt)))
           (write-receipt-path (or (param write-receipt
                                          :then (parse-namestring write-receipt))
                                   read-receipt-path))
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
            :read-receipt-path read-receipt-path
            :write-receipt-path write-receipt-path))))

(defun dump-resultset (stream resultset incremental comparator)
  (iter (for row = (next-row resultset))
        (while row)
        (format stream #?"~{~A~^\t~}~%" row)
        (when incremental
          (for val = (elt row incremental))
          (reducing val by (lambda (a b) (if (funcall comparator a b) b a))))))

(defparameter *type-comparator-map*
  (plist-hash-table (list :int      #'<
                          :long     #'<
                          :float    #'<
                          :double   #'<
                          :decimal  #'<
                          :date     #'string<
                          :time     #'string<
                          :datetime #'string<
                          :string   #'string<)))

(defun get-type-comparator (type)
  (or (gethash type *type-comparator-map*)
      (error "Don't know how to compare ~As" type)))

(defun format-schema-string (metadata)
  (iter (for (col . type) in metadata)
        (reducing (format nil "~A:~A"
                          col
                          (keyword-to-string type))
                  by (lambda (a b) (format nil "~A, ~A" a b)))))

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
          :last-value (or new-value last-value)
          :schema (format-schema-string metadata))))

(defun read-receipt (path)
  (when-let ((ini (read-ini-file path)))
    (iter (for (option . value) in (py-configparser:items ini "receipt"))
          (appending (list (string-to-keyword option) value)))))

(defun write-receipt (path items-plist)
  (let ((ini (py-configparser:make-config)))
    (py-configparser:add-section ini "receipt")
    (iter (for (option value) on items-plist by #'cddr)
          (py-configparser:set-option ini "receipt"
                                      (keyword-to-string option)
                                      value))
    (write-ini-file ini path)))

(defun make-config (options)
  (let* ((config-path (getconf (list options :env) :config))
         (ini (when config-path
                (read-ini-file config-path))))
    (list options ini :env)))

(defun main (&rest argv)
  (let ((options (parse-options argv)))
    (if (getconf options :help)
      (usage)
      (bind ((config (make-config options))
             ((:plist uri table columns limit incremental last-value
                      output-path read-receipt-path write-receipt-path)
              (prepare-parameters config))
             (receipt (when read-receipt-path
                        (read-receipt read-receipt-path))))
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
              (when write-receipt-path
                (write-receipt write-receipt-path new-receipt)))))))))

(defun usage ()
  (print-usage-summary "Parameters:~%~@{~A~%~}~%" +options+))

(defun exec ()
  (handler-case
      (apply #'main (cli-options))
    (error (e)
      (format *error-output* "Dipper: ~A~%~%" e)
      (terminate-process -1))))
