;;;; dot-mca.lisp - Code for parsing region (.mca and .mcr) files.

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
    :chunks (make-array +total-chunks+ :initial-element nil))
  (:documentation "Contents of a region file."))

(defun read-mca-file (mca-file)
  (with-open-file (in mca-file :direction :input
		      :element-type '(unsigned-byte 8))
    (let ((rgn (make-instance 'region)))
      (read-mca-header in rgn)
      (read-mca-payload in rgn)
      )))

(defun read-mca-header (stream rgn)
  (file-position stream 0)
  (with-slots (offsets sector-counts timestamps) rgn
    (dotimes (i +total-chunks+)
      (setf (aref offsets i) (read-si3 stream)
	    (aref sector-counts i) (read-si1 stream)))
    (dotimes (i +total-chunks+)
      (setf (aref timestamps i) (read-si4 stream))))
  rgn)


(defun read-mca-payload (stream rgn)
  (dotimes (i +total-chunks+)
    (load-ith-chunk stream i rgn))
  rgn)

(defun load-ith-chunk (stream i region)
  (with-slots (offsets chunks) region  
    (let ((offset (aref offsets i)))
      (when (chunk-exists-p i region)
	(file-position stream (+ 5 (* +page-size+ offset)))
	(setf (aref chunks i)
	      (read-tag (chipz:make-decompressing-stream
			 'chipz:zlib stream)))))))

(defun chunk-exists-p (i region)
  (with-slots (offsets sector-counts) region
    (let ((offset (aref offsets i))
	  (sector-count (aref sector-counts i)))
      (not (and (zerop offset)
		(zerop sector-count))))))

(defun write-mca-payload (stream rgn)
  (let ((count 0))
    (dotimes (i +total-chunks+)
      (and (dump-ith-chunk-aux stream rgn i)
	   (incf count)))
    count))
(defun dump-ith-chunk-aux (stream rgn i)
  (let ((chunk (chunk-loaded-p i rgn)))
    (when chunk
      (dump-ith-chunk stream
		      i
		      (salza2:compress-data
		       (tag-to-octets chunk)
		       'salza2:zlib-compressor)
		      rgn)
      t)))

(defun chunk-loaded-p (i region)
  (with-slots (chunks) region    
    (let ((chunk (aref chunks i)))
      (if (typep chunk 'nbt-tag)
	  chunk
	  nil))))

(defun write-mca-file (mca-file rgn)
  (with-open-file (out mca-file
		       :direction :output
		       :element-type '(unsigned-byte 8)
		       :if-does-not-exist :create
                       :if-exists :supersede)
    (write-mca-header out rgn)
    (write-mca-payload out rgn)))
;;not necessary since writing the chunk already does this
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
  (with-slots (offsets sector-counts) region
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

(defmacro unwind-protect-abort (protected normal abort)
  (let ((left-correctly (gensym)))
    `(let ((,left-correctly nil))
       (unwind-protect
	    (progn
	      ,protected
	      (setf ,left-correctly t))
	 (if ,left-correctly
	     ,normal
	     ,abort)))))

(defclass handle ()
  ((data :initarg :data :accessor data)
   (handle :initarg :data :accessor handle)
   (path :initarg :path :accessor path))
  (:default-initargs
   :data (make-instance 'region)))
(defun open-handle (path)
  (let ((inst
	 (make-instance 'handle :path path))
	(handle (open path
		      :direction :io
		      :element-type '(unsigned-byte 8)
		      :if-does-not-exist :error
		      :if-exists :overwrite)))
    (unwind-protect-abort 
     (progn
       (setf (handle inst) handle)
       (trivial-garbage:finalize inst (lambda () (close handle)))
       (let ((rgn (slot-value inst 'data)))
	 (read-mca-header handle rgn)))
     nil
     (close handle))
    inst))
(defun close-handle (handle)
;  #+nil
  (close (handle handle)))

(defun flushall (handle)
  (let ((rgn (slot-value handle 'data)))
    (write-mca-payload (handle handle) rgn)))

(defun pos-intra-chunkid (x z)
  (+ (mod x 32) (* (mod z 32) 32)))
(defun load-chunk (x z handle)
  (load-ith-chunk (handle handle)
		  (pos-intra-chunkid x z)
		  (data handle)))
(defun save-chunk (x z handle)
  (dump-ith-chunk (handle handle)
		  (pos-intra-chunkid x z)
		  
		  (data handle)))

(defparameter *region*
  nil)

(defun atest ()
  (let ((handle (open-handle "/home/imac/.minecraft/saves/dogeworld/region/r.0.0.mcr")))
    (setf *region* handle)
    (test6)
    #+nil
    (dotimes (x 64)
      (dotimes (z 64)
	(dotimes (y 64)
	  (setblock (cond ((evenp y) 1)
			  ((evenp x) 2)
			  ((evenp z) 3)
			  (t 4))
		    x y z))))
    (map nil
	 (lambda (x)
	   (when x
	     (recalculate-heights x)))
	 (slot-value (data *region*) 'chunks))
    (print (flushall handle))
    (close-handle handle)))
(defun test ()
  (map nil
       (lambda (x)
	 (when x
	   (let ((y (payload (get-tag x "Level" "Blocks"
				      ))))
	     (map-into y (lambda (x)
			   (if (= 1 x) 0 x))
		       y))))
       (slot-value (data *region*) 'chunks)))


(defun mag (id name payload)
  (let ((inst
	 (instantiate-tag id)))
    (setf (name inst) name
	  (payload inst) payload)
    inst))

(defun new-mcr-chunk (xpos zpos)
  (mag 10 nil
       (list
	(mag 10 "Level"
	     (list
	      (mag 3 "zPos" zpos)
	      (mag 7 "Blocks" (make-array 32768 :element-type '(unsigned-byte 8) :initial-element 0))
	      (mag 7 "SkyLight" (make-array 16384 :element-type '(unsigned-byte 8); :initial-element
					   ; 255
					    ))
	      (mag 7 "HeightMap" (make-array 256 :element-type '(unsigned-byte 8) :initial-element 0))
	      (mag 4 "LastUpdate" 0)
	      (mag 7 "BlockLight" (make-array 16384 :element-type '(unsigned-byte 8)))
	      (mag 7 "Data" (make-array 16384 :element-type '(unsigned-byte 8)))
	      (mag 3 "xPos" xpos)
	      (mag 1 "TerrainPopulated"
		   0 ;;generate natural resources
		   ;;1 dont generate natural stuff
		   )
	      (mag 9 "TileEntities" 1)
	      (mag 9 "Entities" 1))))))

(defun set-chunk (new-tag x z region)
  (setf (aref (slot-value (data region)
			  'chunks)
	      (pos-intra-chunkid x z))
	new-tag))

(defun chunk-mcr (x z)
  (values (floor x 32)
	  (floor z 32)))
(defun pos-chunk (x z)
  (values (floor x 16)
	  (floor z 16)))

(defun getblock (x y z)
  (setf y (mod y 128))
  (multiple-value-bind (chunkx chunkz) (pos-chunk x z)
    (let ((chunks (slot-value (data *region*) 'chunks)))
      (let ((intraid (pos-intra-chunkid chunkx chunkz)))
	(let ((chunk (aref chunks intraid)))
	  (unless chunk
	    (setf chunk (new-mcr-chunk chunkx chunkz))
	    (set-chunk chunk chunkx chunkz *region*))
	  (let ((data
		 (payload (get-tag chunk "Level" "Blocks"))))
	    (aref data (array-lookup x y z))))))))

(defun array-lookup (x y z)
  (+ y
     (* 128 (mod z 16))
     (* 128 16 (mod x 16))))

(defun recalculate-heights (chunk)
  (let ((height (payload (get-tag chunk "Level" "HeightMap")))
	(blocks (payload (get-tag chunk "Level" "Blocks"))))
    (dotimes (x 16)
      (dotimes (z 16)
	(setheigt (getheight x z blocks) x z height)))))

(defun getheight (x z array)
  (let ((num 0))
    (loop for y from 127 downto 0 do
	 (unless (zerop (aref array (array-lookup x y z)))
	   (setf num (min 127 (1+ y)))
	   (return)))
    num))

(defun setheigt (value x z data)
  (setf 
   (aref data (+ (mod x 16)
		 (* 16 (mod z 16))))
   value))

(defun setblock (value x y z)
  (setf y (mod y 128))
  (multiple-value-bind (chunkx chunkz) ;;chunk coordinates
      (pos-chunk x z)
    (let ((chunks (slot-value (data *region*) 'chunks)))
      (let ((intraid (pos-intra-chunkid chunkx chunkz)))
	(let ((chunk (aref chunks intraid)))
	  (unless chunk
	    (setf chunk (new-mcr-chunk chunkx chunkz))
	    (set-chunk chunk chunkx chunkz *region*))
	  (let ((data
		 (payload (get-tag chunk "Level" "Blocks"))))
	    (setf (aref data (array-lookup x y z))
		  value)))))))
(defun setlight (value x y z)
  (setf y (mod y 128))
  (multiple-value-bind (chunkx chunkz) ;;chunk coordinates
      (pos-chunk x z)
    (let ((chunks (slot-value (data *region*) 'chunks)))
      (let ((intraid (pos-intra-chunkid chunkx chunkz)))
	(let ((chunk (aref chunks intraid)))
	  (unless chunk
	    (setf chunk (new-mcr-chunk chunkx chunkz))
	    (set-chunk chunk chunkx chunkz *region*))
	  (let ((data
		 (payload (get-tag chunk "Level" "BlockLight")))
		(place (array-lookup x y z)))
	    (let ((place2 (ash place -1)))
	      (let ((old (aref data place2)))
		(setf (aref data place2)
		      (if (oddp place)
			  (dpb value (byte 4 4) old)
			  (dpb value (byte 4 0) old)))))))))))
(defun setskylight (value x y z)
  (setf y (mod y 128))
  (multiple-value-bind (chunkx chunkz) ;;chunk coordinates
      (pos-chunk x z)
    (let ((chunks (slot-value (data *region*) 'chunks)))
      (let ((intraid (pos-intra-chunkid chunkx chunkz)))
	(let ((chunk (aref chunks intraid)))
	  (unless chunk
	    (setf chunk (new-mcr-chunk chunkx chunkz))
	    (set-chunk chunk chunkx chunkz *region*))
	  (let ((data
		 (payload (get-tag chunk "Level" "SkyLight")))
		(place (array-lookup x y z)))
	    (let ((place2 (ash place -1)))
	      (let ((old (aref data place2)))
		(setf (aref data place2)
		      (if (oddp place)
			  (dpb value (byte 4 4) old)
			  (dpb value (byte 4 0) old)))))))))))


(defun test6 ()
  (atest::map-all-chunks
   (lambda (x y z)
     (let ((bid (world::getblock x y z))
	   (bid1 (world::getlight x y z))
	   (bid2 (world::skygetlight x y z)))
       (setblock bid x y (+ 128 z))
       (setlight bid1 x y (+ 128 z))
       (setskylight bid2 x y (+ 128 z))))))
(defun test3 ()
  (dotimes (x 2)
    (dotimes (y 2)
      #+nil
      (or
       (load-chunk x y *region*))
      (set-chunk (new-mcr-chunk x y) x y *region*)
      )))
