;;;; tag-classes1.lisp - The nbt-tag base class, a macro for defining its
;;;;                     subclasses, and related utils

(in-package #:cl-nbt)

(defclass nbt-tag ()
  ((id :initarg :id :accessor id)
   (name :initarg :name :accessor name)
   (payload :initarg :payload :accessor payload))
  (:documentation "Base class for all tag types."))

(defvar tagtypes
  '(tag-end tag-byte tag-short tag-int tag-long tag-float tag-double
    tag-byte-array tag-string tag-list tag-compound tag-int-array))

(defvar tagtypes->ids
  (mapcar #'cons tagtypes (loop for x upto 11 collect x)))

(defun tagtype->id (tagtype)
  (or (cdr (assoc tagtype tagtypes->ids))
      (error "no id for tagtype ~a" tagtype)))

(defun id->tagtype (id)
  (or (car (rassoc id tagtypes->ids))
      (error "no tagtype for id ~a" id)))

(defun deftagclass (classname)
  `(defclass ,classname (nbt-tag)
     ((id :allocation :class))
     (:default-initargs
       :id ,(tagtype->id classname)
       :name nil
       :payload nil)))

(defun list-class-defs ()
  (loop for x in tagtypes collect
    (deftagclass x)))

(defmacro define-tag-classes ()
  `(progn ,@(list-class-defs)))
