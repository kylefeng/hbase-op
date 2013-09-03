(ns hbase-op.core
  (:import [org.apache.hadoop.hbase HBaseConfiguration]
           [org.apache.hadoop.hbase.client Put Get Delete HTable HTablePool]
           [org.apache.hadoop.hbase.util Bytes]
           [org.apache.hadoop.conf Configuration]
           [java.net URLEncoder]
           [org.apache.commons.codec.digest DigestUtils]))

;; Maximum size of HTablePool
(def max-pool-size 10)

;; Resusable HTablePool
(def htable-pool (HTablePool. (HBaseConfiguration.) max-pool-size))

(defn get-htable
  "Get an intance of HTable from the pool."
  [table]
  (.getTable htable-pool table))

(defn gen-internal-rowkey
  "Genreate internal rowkey:
   md5Hex(original).substring(0, 5) + \"_\" + urlencode(original)"
  [rowkey]
  (str
   (-> rowkey
       DigestUtils/md5Hex
       (.substring 0 5))
   "_"
   (URLEncoder/encode rowkey "utf-8")))

(defn print-from-table
  "Print a row in a table."
  [table rowkey cf]
  (let [table (get-htable table)
        g (Get. (Bytes/toBytes rowkey))
        r (.get table g)
        nm (.getFamilyMap r (Bytes/toBytes cf))]
        (doseq [[k v] nm]
          (let [qualifier (String. k)]
            (if (= (String. k) "gmt_create")
              (println qualifier ":" (java.util.Date. (Bytes/toLong v)))
              (println qualifier ":" (String. v)))))
        (.close table)))

(defn delete-by-rowkeys
  "Delete some rows in a table"
  [table & rks]
  (let [table (get-htable table)
        deletes (java.util.ArrayList.)]
    (doseq [k rks]
      (.add deletes (Delete. (Bytes/toBytes k))))
    (.delete table deletes)
    (.close table)))



