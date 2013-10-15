(in-package :cl-user)
(defpackage :dipper.uri
  (:use :cl :alexandria :dipper.util)
  (:export :database-uri
           :parse-database-uri
           :database-uri-scheme
           :database-uri-subprotocol
           :database-uri-host
           :database-uri-port
           :database-uri-path
           :database-uri-username
           :database-uri-password
           :database-uri-raw))

(in-package :dipper.uri)

(defstruct database-uri scheme subprotocol host port path username password raw)

(defparameter *database-uri-regex*
  (concatenate 'string
               "^([^:/]+):"                  ; scheme
               "(?:([^:/]+):)?"              ; subprotocol
               "(?://"                       ; the whole authority thing...
               "(?:([^@]+?)(?::([^@]+))?@)?" ;   user:passwd
               "(?:([^@/:]+)(?::(\\d+))?)?"  ;   host:port
               ")?"                          ; end
               "(?:(/?[^/][^:]+))?$"))       ; path

(defun parse-database-uri (uri)
  (with-re-match ((scheme subprotocol username password host port path)
                  *database-uri-regex* uri)
    (make-database-uri :scheme scheme
                       :subprotocol subprotocol
                       :host host
                       :port port
                       :path path
                       :password password
                       :username username
                       :raw uri)))
