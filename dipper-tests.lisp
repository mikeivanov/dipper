(in-package :cl-user)

(defpackage :dipper-tests
  (:use :cl :fiveam :iterate :osicat)
  (:import-from :metabang-bind :bind))

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
                                             :port "1234"
                                             :path "/db"
                                             :raw "mysql://host:1234/db")))

  (is (equalp (dipper.uri::parse-database-uri "mysql://user@host:1234/db")
              (dipper.uri::make-database-uri :scheme "mysql"
                                             :host "host"
                                             :port "1234"
                                             :username "user"
                                             :path "/db"
                                             :raw "mysql://user@host:1234/db")))

  (is (equalp (dipper.uri::parse-database-uri "mysql://user:123@host:1234/db")
              (dipper.uri::make-database-uri :scheme "mysql"
                                             :host "host"
                                             :port "1234"
                                             :username "user"
                                             :password "123"
                                             :path "/db"
                                             :raw "mysql://user:123@host:1234/db"))))

(test parse-options
  (bind (((:plist uri table columns limit)
          (dipper::parse-options '("--database" "mysql://host/db"
                                   "--table" "table1"))))
    (is (equalp uri (dipper.uri::make-database-uri :scheme "mysql"
                                                   :host "host"
                                                   :path "/db"
                                                   :raw "mysql://host/db")))
    (is (equal table "table1"))
    (is (equal columns "*"))
    (is (null limit))))

(test parse-options-username
  (bind (((:plist uri)
          (dipper::parse-options '("--database" "mysql://user1@host/db"
                                   "--table" "table1"
                                   "--username" "user2"))))
    (is (equal "user2" (dipper.uri:database-uri-username uri)))))

(test parse-options-passwd
  (bind (((:plist uri)
          (dipper::parse-options '("--database" "mysql://user:passwd@host/db"
                                   "--table" "table1"
                                   "--password" "PASSWD"))))
    (is (equal "PASSWD" (dipper.uri:database-uri-password uri)))))

(test parse-options-output
  (bind (((:plist output-path)
          (dipper::parse-options '("--database" "mysql://user:passwd@host/db"
                                   "--table" "table1"
                                   "--output" "outfile"))))
    (is (equal #p"outfile" output-path))))

(test parse-options-receipt
  (bind (((:plist receipt-path)
          (dipper::parse-options '("--database" "mysql://user:passwd@host/db"
                                   "--table" "table1"
                                   "--receipt" "rfile"))))
    (is (equal #p"rfile" receipt-path))))

(test parse-options-requires-table
  (signals error
    (dipper::parse-options '("--database" "mysql://host/db")))
  (signals error
    (dipper::parse-options '("--table" "foo"))))

(defun make-temp-dir ()
  (let* ((name (format nil "/tmp/dipper-test-~A/" (uuid:make-v4-uuid)))
         (path (parse-namestring name)))
    (ensure-directories-exist path)))

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

(eval-when (:execute)
  (let ((*DEBUG-ON-ERROR* T))
    (run!)))
