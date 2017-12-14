;;;; dot-mca.lisp - Code for parsing region (.mca) files.

(in-package #:cl-nbt)

(defconstant +page-size+ 4096)
(defconstant +total-chunks+ 1024)

(defclass region ()
  ((offsets :initarg :offsets :accessor offsets)
   (sector-counts :initarg :sector-counts :accessor sector-counts)
   (timestamps :initarg :timestamps :accessor timestamps)
;;   (len-chunk-data :initarg :len-chunk-data :accessor len-chunk-data)
;;   (compression-type :initarg :compression-type :accessor compression-type)
   (chunks :initarg :chunks :accessor chunks))
  (:default-initargs
    :offsets (make-array +total-chunks+ :initial-element 0)
    :sector-counts (make-array +total-chunks+ :initial-element 0)
    :timestamps (make-array +total-chunks+ :initial-element 0)
    :chunks (make-array +total-chunks+))
  (:documentation "Contents of a region file."))

;; Verify whether read-mca-file and write-mca-file preserve chunk entry order.

(defun read-mca-file (mca-file)
  (with-open-file (in mca-file :direction :input
		      :element-type '(unsigned-byte 8))
    (let ((rgn (make-instance 'region)))
      (read-mca-header in rgn)
      (read-mca-payload in rgn)
      )))

(defun read-mca-header (stream rgn)
  (with-slots (offsets sector-counts timestamps) rgn
    (dotimes (i +total-chunks+)
      (setf (aref offsets i) (read-si3 stream)
	    (aref sector-counts i) (read-si1 stream)))
    (dotimes (i +total-chunks+)
      (setf (aref timestamps i) (read-si4 stream))))
  rgn)

(defun read-mca-payload (stream rgn)
  (with-slots (offsets sector-counts chunks) rgn
    (loop
       for i below +total-chunks+
       and offset across offsets
       and sector-count across sector-counts
       do (when (not (and (zerop offset) (zerop sector-count)))
	    (file-position stream (+ 5 (* +page-size+ offset)))
	    (setf (aref chunks i)
		  (read-tag (chipz:make-decompressing-stream
			     'chipz:zlib stream))))))
  rgn)

(defun write-mca-file (mca-file rgn)
  (with-open-file (out mca-file
		       :direction :output
		       :element-type '(unsigned-byte 8)
		       :if-does-not-exist :create
                       :if-exists :supersede)
    (write-mca-header out rgn)
    (write-mca-payload out rgn)))

(defun write-mca-header (stream rgn)
  (with-slots (offsets sector-counts timestamps
		       len-chunk-data compression-type) rgn
    (loop
       for offset across offsets
       and sector-count across sector-counts
       do (write-si3 stream offset)
	 (write-si1 stream sector-count))
    (loop
       for timestamp across timestamps
       do (write-si4 stream timestamp))))

(defun round-up-to-page (n)
  (* +page-size+ (ceiling n +page-size+)))

(defun padcount (n)
  (- (round-up-to-page n)
     n))

(defun pad-to-page (stream)
  (let ((pos (file-position stream)))
    (loop repeat (padcount pos)
       do (write-byte 0 stream))))

(defun write-mca-payload (stream rgn)
  (with-slots (offsets sector-counts chunks) rgn
    (loop
       for i below +total-chunks+
       and chunk across chunks
       and offset across offsets
       and sector-count across sector-counts
       do (when (typep chunk 'nbt-tag)
	    (let* ((data (salza2:compress-data
			  (tag-to-octets chunk)
			  'salza2:zlib-compressor))
		   (data-size (length data))
		   (ceil (ceiling (+
				   1 ;;compression type
				   4 ;;chunk length in bytes
				   data-size ;;compressed data
				   )
				  +page-size+)))
	      (pad-to-page stream)
	      
	      ;; Go back and edit sector-count value in header (if necessary)
	      (when (not (= sector-count ceil))
		(let ((prev-pos (file-position stream)))
		  (file-position stream (+ 3 (* i 4)))
		  (write-si1 stream ceil)
		  (file-position stream prev-pos)))

	      ;;go back and edit the offset
	      (let ((prev-pos (file-position stream)))
		(let ((kb-offset (floor prev-pos +page-size+)))
		  (when (not (= offset kb-offset))
		    (file-position stream (* i 4))
		    (write-si3 stream kb-offset)
		    (file-position stream prev-pos))))

	      (progn
		(write-si4 stream (1+ data-size))
		(write-si1 stream
					;1;;gzip
			   2;;zlib
			   ))
	      ;; Write compressed chunk data
	      (write-sequence data stream)

	      ;; Pad up to end of entry
	      (pad-to-page stream))))

    ;; Add padding so file length is a multiple of 4096
    (pad-to-page stream)))

(set-pprint-dispatch
 `(vector * 1024)
 nil
 #+nil
 (lambda (stream obj)
   (dotimes (x 32)
     (terpri stream)
     (dotimes (y 32)
       (princ (row-major-aref obj (+ y (* 32 x))) stream)
       (unless (= 7 31) (princ " " stream))))))
