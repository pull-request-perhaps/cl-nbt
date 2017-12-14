;;;; tag-io.lisp - Readers and writers for nbt-tags of all types

(in-package #:cl-nbt)

;;; Readers

(defun read-header (stream)
  "Read an id and name from STREAM, and return a tag initialized with those
   values."
  (let ((tag (instantiate-tag (read-si1 stream))))
    (with-slots (name) tag
      (when (not (typep tag 'tag-end))
	(let ((x (read-utf-8-string stream (read-si2 stream))))
	  (setf name (if (string= x "") nil x)))))
    tag))

(defgeneric read-payload (stream tag)
  (:documentation
    "Read payload from STREAM and bind its value to TAG's payload slot."))

(defun read-tag (stream)
  "Read a tag from STREAM and return it."
  (read-payload stream (read-header stream)))

(defmethod read-payload (stream (tag tag-end))
  (declare (ignore stream)) ;tag-end has null payload, so do nothing.
  tag)

(defmethod read-payload (stream (tag tag-byte))
  (setf (payload tag) (read-si1 stream))
  tag)

(defmethod read-payload (stream (tag tag-short))
  (setf (payload tag) (read-si2 stream))
  tag)

(defmethod read-payload (stream (tag tag-int))
  (setf (payload tag) (read-si4 stream))
  tag)

(defmethod read-payload (stream (tag tag-long))
  (setf (payload tag) (read-si8 stream))
  tag)

(defmethod read-payload (stream (tag tag-float))
  (setf (payload tag) (read-f4 stream))
  tag)

(defmethod read-payload (stream (tag tag-double))
  (setf (payload tag) (read-f8 stream))
  tag)


(defmethod read-payload (stream (tag tag-byte-array))
  (let* ((len (read-si4 stream))
         (arr (make-array len :element-type '(unsigned-byte 8))))
					;specify element type?
    (dotimes (i len)
      (setf (aref arr i) (read-unsigned-int stream 1)))
    (setf (payload tag) arr))
  tag)

(defmethod read-payload (stream (tag tag-string))
  (setf (payload tag)
        (read-utf-8-string stream (read-si2 stream)))
  tag)

;; If a tag-list is found to be empty, store in its payload slot the
;; de-facto element id of its non-existent entries (either 0 or 1).

(defmethod read-payload (stream (tag tag-list))
  (let* ((element-id (read-si1 stream))
         (num-elements (read-si4 stream)))
    (with-slots (payload) tag
      (cond ((zerop num-elements)
             (setf payload element-id))
            (t
             (setf payload
		   (loop
		      for i below num-elements
		      collect (read-payload
			       stream
			       (instantiate-tag element-id)))))))
    tag))

(defmethod read-payload (stream (tag tag-compound))
  (setf (payload tag)
	(loop
	   for x = (read-tag stream)
	   while (not (typep x 'tag-end))
	   collect x))
  tag)

(defmethod read-payload (stream (tag tag-int-array))
  (let* ((len (read-si4 stream))
         (arr (make-array len :element-type '(signed-byte 32))))
    (dotimes (i len)
      (setf (aref arr i) (read-si4 stream)))
    (setf (payload tag) arr))
  tag)

;;; Writers

(defgeneric write-header (stream tag)
  (:documentation
    "Write to STREAM the id of TAG, and (unless it is a tag-end or is within a
     tag-list) it's name."))

(defmethod write-header (stream (tag nbt-tag))
  (write-si1 stream (id tag))
  (cond ((name tag)
	 (write-si2 stream
		    (length (babel:string-to-octets (name tag)
						    :encoding :utf-8)))
	 (write-utf-8-string stream (name tag)))
	(t
	 (write-si2 stream 0)
	 (write-utf-8-string stream ""))))

(defmethod write-header (stream (tag tag-end))
  (write-si1 stream (id tag)))

(defgeneric write-payload (stream tag)
  (:documentation "Write TAG's payload to STREAM."))

(defun write-tag (stream tag)
  "Write TAG to STREAM."
  (write-header stream tag)
  (write-payload stream tag))

(defmethod write-payload (stream (tag tag-end))
  (declare (ignore stream tag))
  (values))

(defmethod write-payload (stream (tag tag-byte))
  (write-si1 stream (payload tag)))

(defmethod write-payload (stream (tag tag-short))
  (write-si2 stream (payload tag)))

(defmethod write-payload (stream (tag tag-int))
  (write-si4 stream (payload tag)))

(defmethod write-payload (stream (tag tag-long))
  (write-si8 stream (payload tag)))

(defmethod write-payload (stream (tag tag-float))
  (write-f4 stream (payload tag)))

(defmethod write-payload (stream (tag tag-double))
  (write-f8 stream (payload tag)))

;; Simple version (not compatible with .mca files)


(defmethod write-payload (stream (tag tag-byte-array))
  (let ((payload (payload tag)))
    (write-si4 stream (length payload))
    (write-sequence payload stream)))


(defmethod write-payload (stream (tag tag-string))
  (write-si2 stream
	     (length (babel:string-to-octets (payload tag)
					     :encoding :utf-8)))
  (write-utf-8-string stream (payload tag)))

(defmethod write-payload (stream (tag tag-list))
  (with-slots (payload) tag
    (cond ((not (consp payload))
           (write-si1 stream payload)
           (write-si4 stream 0))
          (t
           (write-si1 stream (id (first payload)))
           (write-si4 stream (length payload))
           (loop
	      for x in payload
	      do (write-payload stream x))))))

(defmethod write-payload (stream (tag tag-compound))
  (loop
     for x in (payload tag)
     do (write-tag stream x))
  (write-tag stream (instantiate-tag 0)))

(defmethod write-payload (stream (tag tag-int-array))
  (write-si4 stream (length (payload tag)))
  (loop
     for x across (payload tag)
     do (write-si4 stream x)))

(defun octets-to-tag (seq)
  "Convert the octet vector SEQ to the TAG that it represents."
  (cl-fad:with-open-temporary-file (temp :direction :io
                                         :element-type '(unsigned-byte 8))
    (write-sequence seq temp)
    (file-position temp 0)
    (read-tag temp)))

(defun tag-to-octets (tag)
  "Convert TAG to an octet vector."
  (cl-fad:with-open-temporary-file (temp :direction :io
                                         :element-type '(unsigned-byte 8))
    (write-tag temp tag)
    (file-position temp 0)
    (let ((seq (make-array (file-length temp)
			   :element-type '(unsigned-byte 8))))
      (read-sequence seq temp)
      seq)))

;; See the anomaly in the region file spec:
;; https://minecraft.gamepedia.com/Chunk_format#Block_format

;; The "Blocks" case and the catch-all are almost identical. Refactor?
#+nil
(defmethod read-payload (stream (tag tag-byte-array))
  (cond ((member (name tag)
                 '("BlockLight" "SkyLight" "Add" "Data")
	         :test #'string=)
	 (let* ((len (read-si4 stream))
		(nibbles (make-array (* 2 len) :element-type '(unsigned-byte 4))))
	   (loop
	      for i below len
	      for j = (* 2 i)
	      for octet = (read-unsigned-int stream 1)
	      do (setf (aref nibbles j)
		       (logand #b00001111 octet)
		       (aref nibbles (1+ j))
		       (ash (logand #b11110000 octet) -4)))
	   (setf (payload tag) nibbles)))
	((string= (name tag) "Blocks")
	 (let* ((len (read-si4 stream))
                (arr (make-array len :element-type '(unsigned-byte 8))))
           (dotimes (i len)
             (setf (aref arr i) (read-unsigned-int stream 1)))
           (setf (payload tag) arr)))
	(t
	 (let* ((len (read-si4 stream))
                (arr (make-array len :element-type '(signed-byte 8))))
           (dotimes (i len)
             (setf (aref arr i) (read-si1 stream)))
           (setf (payload tag) arr))))
  tag)


;; See comment above tag-byte-array's read-payload method

#+nil
(defmethod write-payload (stream (tag tag-byte-array))
  (progn (write-si4 stream (length (payload tag)))
		(write-sequence (payload tag) stream))
  (cond ((member (name tag)
		 '("BlockLight" "SkyLight" "Add" "Data")
		 :test #'string=)
	 (let ((octets (make-array 2048 :element-type '(unsigned-byte 8))))
	   (loop
	      for i below 4096 by 2
	      for j below 2048
	      do (setf (aref octets j)
		       (+ (aref (payload tag) i)
			  (ash (aref (payload tag) (1+ i)) 4))))
	   (write-si4 stream 2048)
	   (write-sequence octets stream)))
	(t
	 )))
