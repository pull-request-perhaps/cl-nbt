;;;; cl-nbt.asd

(asdf:defsystem #:cl-nbt
  :description "Library for parsing Minecraft's binary files."
  :author "Andy Page"
  :license "MIT"
  :depends-on (#:alexandria
               #:ieee-floats
               #:babel
               #:chipz
               #:salza2
               #:cl-fad
	       #:trivial-garbage)
  :serial t
  :components ((:file "package")
               (:file "tag-classes1")
               (:file "tag-classes2")
               (:file "binary-io")
               (:file "tag-io")
               (:file "dot-dat")
               (:file "dot-mca")))
