(in-package :cl-user)

(defpackage :dipper-tests
  (:use :cl :fiveam :iterate :osicat)
  (:import-from :metabang-bind :bind)
  (:import-from :alexandria :plist-hash-table))

(in-package :dipper-tests)

(def-suite :dipper)
(in-suite :dipper)

(test parse-database-uri
  (is (equalp (dipper.uri::parse-database-uri "mysql://host")
              (dipper.uri::make-database-uri :scheme "mysql"
                                             :host "host"
                                             :raw "mysql://host")))

  (is (equalp (dipper.uri::parse-database-uri "mysql://host/db")
              (dipper.uri::make-database-uri :scheme "mysql"
                                             :host "host"
                                             :path "/db"
                                             :raw "mysql://host/db")))

  (is (equalp (dipper.uri::parse-database-uri "mysql://host:1234/db")
              (dipper.uri::make-database-uri :scheme "mysql"
                                             :host "host"
                                             :port 1234
                                             :path "/db"
                                             :raw "mysql://host:1234/db")))

  (is (equalp (dipper.uri::parse-database-uri "mysql://user@host:1234/db")
              (dipper.uri::make-database-uri :scheme "mysql"
                                             :host "host"
                                             :port 1234
                                             :username "user"
                                             :path "/db"
                                             :raw "mysql://user@host:1234/db")))

  (is (equalp (dipper.uri::parse-database-uri "mysql://user:123@host:1234/db")
              (dipper.uri::make-database-uri :scheme "mysql"
                                             :host "host"
                                             :port 1234
                                             :username "user"
                                             :password "123"
                                             :path "/db"
                                             :raw "mysql://user:123@host:1234/db"))))

(test parse-database-uri-override
  (let ((uri (dipper.uri::parse-database-uri "mysql://user@host"
                                             :override (list :username "mr.x"))))
    (is (equal (dipper.uri::database-uri-username uri) "mr.x"))))

(test parse-options
  (let ((options (dipper::parse-options '("--database" "mysql://host/db"
                                          "--table" "table1"))))
    (is (typep options 'hash-table))
    (is (equal (gethash :database options) "mysql://host/db"))
    (is (equal (gethash :table options) "table1"))
    (is (equal (gethash :username options) nil))))

(test make-config
  (let* ((options (dipper::parse-options '("--database" "mysql://host/db"
                                          "--table" "table1")))
         (config (dipper::make-config options)))
    (is (equal (dipper::getconf config :database) "mysql://host/db"))
    (is (equal (dipper::getconf config :table) "table1"))
    (is (equal (dipper::getconf config :incremental) nil))))

(defun make-temp-dir ()
  (let* ((name (format nil "/tmp/dipper-test-~A/" (uuid:make-v4-uuid)))
         (path (parse-namestring name)))
    (ensure-directories-exist path)))

(def-fixture sandbox ()
  (let ((sandbox (make-temp-dir)))
    (&body)
    (delete-directory-and-files sandbox)))

(test make-config-precedence
  (with-fixture sandbox ()
    (let ((cfg-path (merge-pathnames #p"cfg" sandbox)))
      (with-open-file (cfg cfg-path
                           :direction :output
                           :if-does-not-exist :create)
        (format cfg "[dipper]~%database=ini~%table=ini~%incremental=ini~%"))
      (let* ((options (dipper::parse-options (list "--database" "arg"
                                                   "--table" "arg"
                                                   "--limit" "arg"
                                                   "--config" (namestring cfg-path))))
             (config (dipper::make-config options)))
        (setf (environment-variable "DIPPER_LIMIT") "env")
        (setf (environment-variable "DIPPER_RECEIPT") "env")
        (unwind-protect
             (progn
               (is (equal (dipper::getconf config :database) "arg"))
               (is (equal (dipper::getconf config :table) "arg"))
               (is (equal (dipper::getconf config :limit) "arg"))
               (is (equal (dipper::getconf config :incremental) "ini"))
               (is (equal (dipper::getconf config :receipt) "env"))
               (is (equal (dipper::getconf config :last-value) nil)))
          (progn
            (makunbound-environment-variable "DIPPER_LIMIT")
            (makunbound-environment-variable "DIPPER_RECEIPT")))))))

(test prepare-parameters
  (let* ((config (plist-hash-table '(:database "mysql://host/db"
                                     :table "table1"
                                     :output "outfile"
                                     :receipt "recfile")))
         (params (dipper::prepare-parameters config)))
    (is (typep (getf params :uri) 'dipper.uri::database-uri))
    (is (equal (getf params :table) "table1"))
    (is (equal (getf params :output-path) #p"outfile"))
    (is (equal (getf params :receipt-path) #p"recfile"))
    (is (equal (getf params :incremental) nil))))

(test prepare-parameters-default
  (let* ((config (plist-hash-table '(:database "mysql://host/db"
                                     :table "table1"
                                     :incremental "DEFAULT"
                                     :columns "DEFAULT")))
         (params (dipper::prepare-parameters config)))
    (is (equal (getf params :incremental) nil))
    (is (equal (getf params :columns) "*"))))

(test prepare-parameters-requires-table
  (signals error
    (dipper::prepare-parameters (plist-hash-table '(:database "mysql://host/db"))))
  (signals error
    (dipper::prepare-parameters (plist-hash-table '(:table "table")))))

(defun make-test-db (path)
  (let ((db (sqlite:connect path)))
    (sqlite:execute-non-query db "create table things (id integer primary key,
                                                       name text not null)")
    (sqlite:execute-non-query db "insert into things (id, name)
                                                      values (1, 'Thing #1')")
    (sqlite:execute-non-query db "insert into things (id, name)
                                                      values (2, 'Thing #2')")
    (sqlite:disconnect db)
    nil))

(def-fixture db ()
  (let* ((sandbox (make-temp-dir))
         (dbpath (merge-pathnames #p"data.db" sandbox)))
    (make-test-db dbpath)
    (&body)
    (delete-directory-and-files sandbox)))

(defun make-dbspec (path)
  (format nil "sqlite3:~A" (namestring path)))

(def-fixture db-conn ()
  (let* ((sandbox (make-temp-dir))
         (dbpath (merge-pathnames #p"data.db" sandbox)))
    (make-test-db dbpath)
    (let ((uri (dipper.uri::parse-database-uri (make-dbspec dbpath))))
      (dipper.dbi::with-connection (conn uri)
        (&body)))
    (delete-directory-and-files sandbox)))

(test dump-table
  (with-fixture db-conn ()
    (let ((str (with-output-to-string (out)
                 (dipper::dump-table conn "things" out))))
      (is (equal str
                 (format nil "1~TThing #1~%2~TThing #2~%"))))))

(test dump-table-projection
  (with-fixture db-conn ()
    (let ((str (with-output-to-string (out)
                 (dipper::dump-table conn "things" out :columns "id"))))
      (is (equal str
                 (format nil "1~%2~%"))))))

(test dump-table-limit
  (with-fixture db-conn ()
    (let* ((str (with-output-to-string (out)
                  (dipper::dump-table conn "things" out :limit 1)))
           (cnt (iter (for c in-string str)
                      (counting (eql c #\Newline)))))
      (is (equal cnt 1)))))

(test dump-table-incremental
  (with-fixture db-conn ()
    (let* ((str (with-output-to-string (out)
                  (dipper::dump-table conn "things" out
                                      :incremental :id
                                      :last-value 1)))
           (cnt (iter (for c in-string str)
                      (counting (eql c #\Newline)))))
      (is (equal cnt 1)))))

(test dump-table-incremental-string
  (with-fixture db-conn ()
    (let* ((str (with-output-to-string (out)
                  (dipper::dump-table conn "things" out
                                      :incremental :name
                                      :last-value "Thing #1")))
           (cnt (iter (for c in-string str)
                      (counting (eql c #\Newline)))))
      (is (equal cnt 1)))))

(defun slurp-stream (stream)
  (let ((seq (make-array (file-length stream)
                         :element-type 'character
                         :fill-pointer t)))
    (setf (fill-pointer seq) (read-sequence seq stream))
    seq))

(test main-output
  (with-fixture db ()
    (let ((outpath (merge-pathnames #p"output.dat" sandbox)))
      (dipper::main "--database" (make-dbspec dbpath)
                    "--table" "things"
                    "--output" (namestring outpath))
      (with-open-file (f outpath :direction :input)
        (is (equal (slurp-stream f)
                   (format nil "1~TThing #1~%2~TThing #2~%")))))))

(test main-receipt-write
  (with-fixture db ()
    (let ((outpath (merge-pathnames #p"things.dat" sandbox))
          (rcppath (merge-pathnames #p"things.receipt" sandbox)))
      (dipper::main "--database" (make-dbspec dbpath)
                    "--table" "things"
                    "--incremental" "id"
                    "--output" (namestring outpath)
                    "--receipt" (namestring rcppath))
      (with-open-file (f rcppath :direction :input)
        (let ((receipt (yason:parse f
                                    :object-as :plist
                                    :object-key-fn #'dipper.util::string-to-keyword)))
          (is (equal (getf receipt :last-value) 2))
          (is (equal (getf receipt :incremental) "id"))
          (is (equal (getf receipt :table) "things"))
          (is (equal (getf receipt :columns) "*")))))))

(test main-receipt-read
  (with-fixture db ()
    (let ((outpath (merge-pathnames #p"things.dat" sandbox))
          (rcppath (merge-pathnames #p"things.receipt" sandbox)))
      (with-open-file (f rcppath
                         :direction :output
                         :if-does-not-exist :create)
        (format f "{\"LAST-VALUE\": 1}"))
      (dipper::main "--database" (make-dbspec dbpath)
                    "--table" "things"
                    "--incremental" "id"
                    "--output" (namestring outpath)
                    "--receipt" (namestring rcppath))
      (with-open-file (f outpath :direction :input)
        (is (equal (slurp-stream f)
                   (format nil "2~TThing #2~%")))))))

(test help
  (let ((str (with-output-to-string (out)
               (let ((*standard-output* out))
                 (dipper::main "--help")))))
    (is (equal (subseq str 0 11) "Parameters:"))))

(eval-when (:execute)
  (let ((*DEBUG-ON-ERROR* T))
    (run!)))
