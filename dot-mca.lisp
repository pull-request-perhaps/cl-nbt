;;;; dot-mca.lisp - Code for parsing region (.mca) files.

(in-package #:cl-nbt)

(defconstant +page-size+ 4096)
(defconstant +total-chunks+ 1024)

(defclass region ()
  ((offsets :initarg :offsets :accessor offsets)
   (sector-counts :initarg :sector-counts :accessor sector-counts)
   (timestamps :initarg :timestamps :accessor timestamps)
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
  (with-slots (offsets sector-counts timestamps) rgn
    (loop
       for offset across offsets
       and sector-count across sector-counts
       do (write-si3 stream offset)
	 (write-si1 stream sector-count))
    (loop
       for timestamp across timestamps
       do (write-si4 stream timestamp))))

(defun write-mca-payload (stream rgn)
  (with-slots (offsets sector-counts chunks) rgn
    (dotimes (i +total-chunks+)
      (let ((chunk (aref chunks i)))
	(when (typep chunk 'nbt-tag)
	  (dump-ith-chunk stream
			  i
			  (salza2:compress-data
			   (tag-to-octets (aref chunks 0 ;i
						))
			   'salza2:zlib-compressor)
			  rgn))))))

(defun round-up-to-page (n)
  (* +page-size+ (ceiling n +page-size+)))

(defun padcount (n)
  (- (round-up-to-page n)
     n))

(defun pad-to-page (stream)
  (let ((pos (file-position stream)))
    (loop repeat (padcount pos)
       do (write-byte 0 stream))))

(defun dump-ith-chunk (stream i data region)
  (with-slots (offsets sector-counts chunks) region
    (let ((offset (aref offsets i))
	  (sector-count (aref sector-counts i)))
      (let* ((data-size (length data))
	     (new-sector-count (ceiling (+
					 1 ;;compression type
					 4 ;;chunk length in bytes
					 data-size ;;compressed data
					 )
					+page-size+)))   
	(if (= sector-count
	       new-sector-count)
	    ;;write it in the same place, same size as before
	    (file-position stream (* +page-size+ offset))
	    ;; Go back and edit sector-count value in header (if necessary)
	    (progn
	      (setf (aref sector-counts i) new-sector-count)
	      (let ((prev-pos (file-position stream)))
		(file-position stream (+ 3 (* i 4)))
		(write-si1 stream new-sector-count)
		(file-position stream prev-pos))
	      
	      (let ((kb-offset (find-space (intervals region i) new-sector-count)))
		;;go back and edit the offset		    
		(unless (= offset kb-offset)
		  (setf (aref offsets i) kb-offset)
		  (let ((prev-pos (file-position stream)))
		    (file-position stream (* i 4))
		    (write-si3 stream kb-offset)
		    (file-position stream prev-pos)))
		;;go to new area
		(file-position stream (* kb-offset +page-size+)))))

	;;start of data
	(progn
	  (write-si4 stream (1+ data-size))
	  (write-si1 stream
					;1;;gzip
		     2;;zlib
		     ))
	;; Write compressed chunk data
	(write-sequence data stream)

	(pad-to-page stream)))))

(defun intervals (rgn &optional (exclude -1))
  (with-slots (offsets sector-counts) rgn
    (let (acc)
      (loop
	 for i below +total-chunks+
	 and offset across offsets
	 and sector-count across sector-counts
	 do (when (not (and (zerop offset) (zerop sector-count)))
	      (unless (= i exclude)
		(push (cons offset (+ offset sector-count)) acc))))
      (sort acc #'< :key #'first))))

(defun find-space (intervals size)
  (let ((last 2)
	(bailout (1+ +total-chunks+)) ;;in case of error
	)
    (flet ((next-cell ()
	     (if intervals
		 (pop intervals)
		 (load-time-value (cons most-positive-fixnum
					"end")))))
      (block later
	(tagbody
	 again
	   (destructuring-bind (a . b) (next-cell)
	     (if (>= (- a last) size)
		 (return-from later last)
		 (progn
		   (setf last b)
		   (when (zerop (decf bailout))
		     (error "chunks screwed up"))
		   (go again)))))))))



#+nil
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
